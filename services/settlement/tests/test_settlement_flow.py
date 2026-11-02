import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from bid_svc.main import create_app as create_bid_app
from bid_svc.wallet_client import WalletClient
from fastapi.testclient import TestClient
from market_svc.events import apply_event
from market_svc.tick import run_tick
from settlement_svc.ingest import ingest_cleared
from settlement_svc.main import create_app as create_settlement_app
from settlement_svc.models import Item, Kind, State
from settlement_svc.processor import process_pending, reconcile
from settlement_svc.wallet_client import SettlementWallet, Transient
from spotcommon.config import RESERVE_MICRO
from spotcommon.db import Base, make_engine, make_session_factory
from spotcommon.events import InMemoryBus
from spotcommon.money import tick_charge
from spotcommon.outbox import relay_once
from sqlalchemy import select
from wallet_svc.main import create_app as create_wallet_app

TICK_S = 60  # tests run 60s ticks so the minimum bid duration (60s) is 1 tick, 120s is 2
A100 = RESERVE_MICRO["A100"]
# Test clock: a day ahead of the real clock, so freshly created items are already due.
T0 = datetime.now(timezone.utc) + timedelta(days=1)


class FlakyWallet(SettlementWallet):
    """Same wallet, but can be switched 'down' to inject outages."""

    down = False

    def capture(self, *a, **k):
        if self.down:
            raise Transient("wallet down")
        return super().capture(*a, **k)

    def release(self, *a, **k):
        if self.down:
            raise Transient("wallet down")
        return super().release(*a, **k)


class World:
    def __init__(self):
        self.wallet_http = TestClient(create_wallet_app("sqlite://"))
        self.wallet_http.__enter__()
        self.bids = TestClient(create_bid_app("sqlite://", wallet=WalletClient(self.wallet_http)))
        self.bids.__enter__()
        e = make_engine("sqlite://")
        Base.metadata.create_all(e)
        self.mdb = make_session_factory(e)()
        self.settle = TestClient(create_settlement_app("sqlite://", wallet=FlakyWallet(self.wallet_http)))
        self.settle.__enter__()
        self.sf = self.settle.app.state.session_factory
        self.wallet = FlakyWallet(self.wallet_http)
        self.bus, self._fed, self._seq = InMemoryBus(), 0, 0

    def close(self):
        for c in (self.settle, self.bids, self.wallet_http):
            c.__exit__(None, None, None)

    def fund(self, user, micro):
        self.wallet_http.post(f"/v1/wallets/{user}/deposit", json={"amount_micro": micro, "idempotency_key": f"f-{user}"})

    def bid(self, user, qty, price, dur):
        r = self.bids.post("/v1/bids", json={"user_id": user, "gpu_type": "A100", "qty": qty,
                                             "max_price_micro": price, "duration_s": dur}).json()
        assert r["status"] == "ACTIVE", r
        self.deliver_bid_events()
        return r["bid_id"], r["hold_amount_micro"]

    def deliver_bid_events(self):
        asyncio.run(relay_once(self.bids.app.state.session_factory, self.bus, "bid"))
        for topic, _k, payload in self.bus.log[self._fed:]:
            apply_event(self.mdb, topic, payload)
        self._fed = len(self.bus.log)
        self.mdb.commit()

    def tick(self, cap=8):
        t = run_tick(self.mdb, "A100", cap, TICK_S)
        self.mdb.commit()
        return t.result

    def ingest(self, *results):
        with self.sf() as db:
            n = sum(ingest_cleared(db, r) for r in results)
            db.commit()
        return n

    def run(self, now=T0, **kw):
        return process_pending(self.sf, self.wallet, now, **kw)

    def w(self, user):
        return self.wallet_http.get(f"/v1/wallets/{user}").json()

    def hold(self, bid_id):
        return self.wallet_http.get(f"/v1/holds/{bid_id}").json()

    def items(self, **where):
        with self.sf() as db:
            return list(db.scalars(select(Item).where(*[getattr(Item, k) == v for k, v in where.items()]).order_by(Item.id)))

    def integrity(self):
        return self.wallet_http.get("/v1/admin/integrity").json()


@pytest.fixture()
def world(monkeypatch):
    monkeypatch.setenv("TICK_SECONDS", str(TICK_S))  # the bid service sizes escrow from this
    w = World()
    yield w
    w.close()


def test_job_runs_completes_is_charged_per_tick_and_refunded(world):
    world.fund("alice", 50_000_000)
    bid_id, hold = world.bid("alice", qty=8, price=2_000_000, dur=120)  # 2 ticks
    r1, r2 = world.tick(), world.tick()
    assert r2["completed"] == [bid_id]
    assert world.ingest(r1, r2) == 3  # 2 captures + 1 release

    st = world.run()
    assert (st.done, st.retried, st.dead) == (3, 0, 0)

    charges = 2 * tick_charge(A100, 8, TICK_S)  # clearing price was the reserve, not her 2.0 bid
    h = world.hold(bid_id)
    assert (h["captured_micro"], h["released_micro"], h["status"]) == (charges, hold - charges, "CLOSED")
    assert world.w("alice") == {"user_id": "alice", "available_micro": 50_000_000 - charges, "held_micro": 0}
    assert world.integrity()["ok"]
    assert world.settle.post("/v1/admin/reconcile").json() == {"ok": True, "problems": []}
    stats = world.settle.get("/v1/settlement/stats").json()
    assert stats["total_captured_micro"] == charges


def test_replayed_events_and_double_runs_never_double_charge(world):
    world.fund("alice", 50_000_000)
    bid_id, hold = world.bid("alice", qty=8, price=2_000_000, dur=120)
    r1, r2 = world.tick(), world.tick()
    assert world.ingest(r1, r2) == 3
    assert world.ingest(r1, r2) == 0  # Kafka redelivery
    world.run()
    before = world.w("alice")
    st = world.run()  # a second worker / a re-run finds nothing to do
    assert (st.done, st.skipped) == (0, 0) and world.w("alice") == before
    assert world.hold(bid_id)["captured_micro"] == 2 * tick_charge(A100, 8, TICK_S)


def test_wallet_outage_retries_with_backoff_and_release_waits_for_capture(world):
    world.fund("alice", 50_000_000)
    bid_id, hold = world.bid("alice", qty=8, price=2_000_000, dur=60)  # finishes in 1 tick
    world.ingest(world.tick())
    world.wallet.down = True
    st = world.run(T0)
    assert (st.done, st.retried, st.deferred) == (0, 1, 1)  # capture retried; release held back
    assert world.hold(bid_id)["released_micro"] == 0 and world.hold(bid_id)["captured_micro"] == 0
    assert world.run(T0 + timedelta(milliseconds=500)).done == 0  # not due yet (backoff)

    world.wallet.down = False
    st = world.run(T0 + timedelta(minutes=1))
    assert st.done == 2  # capture first, THEN release, in one pass
    charge = tick_charge(A100, 8, TICK_S)
    h = world.hold(bid_id)
    assert (h["captured_micro"], h["released_micro"]) == (charge, hold - charge)
    assert world.integrity()["ok"]


def test_preempted_job_pays_only_for_ticks_it_ran_and_gets_the_rest_back(world):
    world.fund("low", 50_000_000)
    world.fund("whale", 500_000_000)
    low, low_hold = world.bid("low", qty=8, price=1_000_000, dur=3600)
    r1, r2 = world.tick(), world.tick()
    _whale, _ = world.bid("whale", qty=8, price=3_000_000, dur=3600)
    r3 = world.tick()
    assert r3["preempted"] == [low] and r3["clearing_price_micro"] == 1_000_000
    world.ingest(r1, r2, r3)
    world.run()

    ran = 2 * tick_charge(A100, 8, TICK_S)
    h = world.hold(low)
    assert (h["captured_micro"], h["released_micro"], h["status"]) == (ran, low_hold - ran, "CLOSED")
    assert world.w("low")["available_micro"] == 50_000_000 - ran and world.w("low")["held_micro"] == 0
    # whale ran one tick and pays the loser's 1.0/hr, not its own 3.0 bid
    whale_charge = [c for c in r3["charges"] if c["user_id"] == "whale"][0]["charge_micro"]
    assert whale_charge == tick_charge(1_000_000, 8, TICK_S)
    assert world.integrity()["ok"] and world.settle.post("/v1/admin/reconcile").json()["ok"]


def test_cancel_is_refunded_by_settlement_after_the_last_charge_never_skipping_one(world):
    world.fund("alice", 50_000_000)
    bid_id, _ = world.bid("alice", qty=8, price=2_000_000, dur=3600)
    r1 = world.tick()
    assert world.bids.delete(f"/v1/bids/{bid_id}").json()["status"] == "CANCELLED"
    assert world.hold(bid_id)["status"] == "ACTIVE"  # cancelling does not touch the wallet
    r2 = world.tick()  # the job runs once more: the cancel event has not reached the market yet
    world.deliver_bid_events()
    r3 = world.tick()  # now the market knows: it stops billing and announces the cancel
    assert r3["charges"] == [] and r3["cancelled"] == [bid_id] and world.tick()["cancelled"] == []
    world.ingest(r1, r2, r3)
    # the first capture is stuck behind an outage; the release must wait for it (and for the second)
    world.wallet.down = True
    world.run(max_attempts=5)
    world.wallet.down = False
    st = world.run(T0 + timedelta(hours=1))
    assert (st.skipped, st.dead) == (0, 0)
    assert [i.state for i in world.items()] == [State.DONE] * 3
    charged = sum(c["charge_micro"] for r in (r1, r2) for c in r["charges"])
    assert world.hold(bid_id)["captured_micro"] == charged and world.hold(bid_id)["status"] == "CLOSED"
    assert world.w("alice")["available_micro"] == 50_000_000 - charged and world.integrity()["ok"]


def test_exhausted_retries_go_dead_and_keep_blocking_that_bids_release(world):
    world.fund("alice", 50_000_000)
    bid_id, _ = world.bid("alice", qty=8, price=2_000_000, dur=60)
    world.ingest(world.tick())
    world.wallet.down = True
    for i in range(3):
        world.run(T0 + timedelta(hours=i), max_attempts=3)
    cap, rel = world.items()
    assert cap.state == State.DEAD and cap.attempts == 3
    assert rel.state == State.PENDING  # must not refund money whose capture is unresolved
    dead = world.settle.get("/v1/settlement/dead").json()
    assert [d["bid_id"] for d in dead] == [bid_id]


def test_reconcile_catches_drift_between_settlement_and_wallet(world):
    world.fund("alice", 50_000_000)
    bid_id, _ = world.bid("alice", qty=8, price=2_000_000, dur=3600)
    world.ingest(world.tick())
    world.run()
    with world.sf() as db:
        assert reconcile(db, world.wallet) == []
    # someone captures behind settlement's back
    world.wallet_http.post(f"/v1/holds/{bid_id}/capture", json={"amount_micro": 7, "idempotency_key": "rogue"})
    with world.sf() as db:
        problems = reconcile(db, world.wallet)
    assert len(problems) == 1 and bid_id in problems[0]
    assert world.settle.post("/v1/admin/reconcile").json()["ok"] is False


def test_charge_items_precede_release_items_in_order(world):
    world.fund("alice", 50_000_000)
    world.bid("alice", qty=8, price=2_000_000, dur=120)
    world.ingest(world.tick(), world.tick())
    assert [i.kind for i in world.items()] == [Kind.CAPTURE, Kind.CAPTURE, Kind.RELEASE]


def test_celery_task_and_beat_schedule_are_registered():
    from settlement_svc.celery_app import celery

    assert "settlement.process_pending" in celery.tasks
    assert celery.conf.beat_schedule["settle-pending"]["schedule"] == 1.0
    assert celery.conf.task_acks_late is True
