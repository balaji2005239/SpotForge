import asyncio

import pytest
from fastapi.testclient import TestClient
from market_svc.events import apply_event
from market_svc.main import create_app as create_market_app
from market_svc.models import BookBid, Status, Tick
from market_svc.tick import run_tick
from market_svc.worker import tick_once
from spotcommon.config import RESERVE_MICRO
from spotcommon.db import Base, make_engine, make_session_factory
from spotcommon.events import InMemoryBus
from spotcommon.money import hold_amount, tick_charge
from spotcommon.outbox import Outbox, relay_once
from sqlalchemy import select

A100 = RESERVE_MICRO["A100"]  # 800_000
TICK_S = 5


@pytest.fixture()
def db():
    e = make_engine("sqlite://")
    Base.metadata.create_all(e)
    with make_session_factory(e)() as s:
        yield s


def submit(db, bid_id, seq, qty, price, dur=3600, divisible=False, gpu="A100", user="u1"):
    apply_event(db, "bid.submitted", {
        "bid_id": bid_id, "seq": seq, "user_id": user, "gpu_type": gpu, "qty": qty,
        "max_price_micro": price, "duration_s": dur, "divisible": divisible,
        "hold_amount_micro": hold_amount(price, qty, dur, TICK_S)})
    db.commit()


def tick(db, cap=8, gpu="A100", **kw):
    t = run_tick(db, gpu, cap, TICK_S, **kw)
    db.commit()
    return t


def status(db, bid_id):
    return db.get(BookBid, bid_id).status


def topics(db):
    return [o.topic for o in db.scalars(select(Outbox).order_by(Outbox.id))]


def test_job_runs_to_completion_and_is_charged_each_tick(db):
    submit(db, "A", 1, qty=8, price=2_000_000, dur=10)  # 2 ticks
    t1 = tick(db)
    assert status(db, "A") == Status.RUNNING
    assert t1.clearing_price_micro == A100  # nobody lost -> reserve, not the 2.0 bid
    assert t1.result["charges"][0]["charge_micro"] == tick_charge(A100, 8, TICK_S)
    t2 = tick(db)
    assert status(db, "A") == Status.COMPLETED and t2.result["completed"] == ["A"]
    t3 = tick(db)
    assert t3.result["charges"] == []  # nothing left to charge
    assert topics(db).count("allocation.created") == 1 and topics(db).count("allocation.completed") == 1


def test_higher_bid_preempts_running_job_and_pays_the_losers_price(db):
    submit(db, "low", 1, qty=8, price=1_000_000)
    tick(db)
    submit(db, "whale", 2, qty=8, price=3_000_000)
    t = tick(db)
    assert status(db, "whale") == Status.RUNNING and status(db, "low") == Status.PREEMPTED
    assert t.result["preempted"] == ["low"]
    assert t.clearing_price_micro == 1_000_000  # whale bid 3.0 but pays the marginal loser's 1.0
    assert t.result["charges"][0]["charge_micro"] == tick_charge(1_000_000, 8, TICK_S)
    assert "allocation.preempted" in topics(db)


def test_incumbent_keeps_gpus_on_a_tie(db):
    submit(db, "inc", 1, qty=8, price=1_000_000)
    tick(db)
    submit(db, "new", 2, qty=8, price=1_000_000)
    tick(db)
    assert status(db, "inc") == Status.RUNNING and status(db, "new") == Status.QUEUED


def test_queued_bid_waits_then_gets_gpus_when_capacity_grows(db):
    submit(db, "A", 1, qty=8, price=1_000_000)
    submit(db, "B", 2, qty=8, price=900_000)
    tick(db, cap=8)
    assert status(db, "B") == Status.QUEUED
    tick(db, cap=16)
    assert status(db, "B") == Status.RUNNING and status(db, "A") == Status.RUNNING


def test_node_failure_shrinks_capacity_and_preempts_lowest_bidder(db):
    submit(db, "hi", 1, qty=8, price=2_000_000)
    submit(db, "lo", 2, qty=8, price=1_000_000)
    tick(db, cap=16)
    assert status(db, "lo") == Status.RUNNING
    t = tick(db, cap=8)  # a node died
    assert status(db, "hi") == Status.RUNNING and status(db, "lo") == Status.PREEMPTED
    assert t.result["preempted"] == ["lo"]


def test_divisible_running_job_is_resized_not_killed(db):
    submit(db, "div", 1, qty=8, price=1_000_000, divisible=True)
    tick(db, cap=8)
    submit(db, "big", 2, qty=3, price=2_000_000)
    tick(db, cap=8)
    b = db.get(BookBid, "div")
    assert b.status == Status.RUNNING and b.alloc_qty == 5
    assert "allocation.resized" in topics(db)


def test_cancelled_running_job_stops_being_scheduled(db):
    submit(db, "A", 1, qty=8, price=1_000_000)
    tick(db)
    assert apply_event(db, "bid.cancelled", {"bid_id": "A", "seq": 1, "user_id": "u1", "gpu_type": "A100", "qty": 8,
                                             "max_price_micro": 1_000_000, "duration_s": 3600}) == "cancelled_running"
    db.commit()
    assert tick(db).result["charges"] == []


def test_duplicate_tick_number_is_rejected_without_side_effects(db):
    submit(db, "A", 1, qty=8, price=1_000_000)
    assert tick(db, tick_no=1) is not None
    ran = db.get(BookBid, "A").ticks_run
    assert tick(db, tick_no=1) is None  # a second "leader" loses
    assert db.get(BookBid, "A").ticks_run == ran
    assert db.query(Tick).count() == 1


def test_markets_are_independent(db):
    submit(db, "a", 1, qty=8, price=2_000_000, gpu="A100")
    submit(db, "h", 2, qty=8, price=2_000_000, gpu="H100")
    tick(db, gpu="A100")
    assert status(db, "a") == Status.RUNNING and status(db, "h") == Status.QUEUED


def test_total_charges_never_exceed_the_escrow_even_with_fluctuating_prices(db):
    price_cap, qty, dur = 1_500_000, 4, 60
    submit(db, "J", 1, qty=qty, price=price_cap, dur=dur)
    escrow, spent, n = hold_amount(price_cap, qty, dur, TICK_S), 0, 0
    while status(db, "J") == Status.RUNNING or n == 0:
        n += 1
        if n % 2 == 0:  # a competitor that can't fit in the leftover 4 GPUs but sets the price
            submit(db, f"c{n}", 100 + n, qty=8, price=900_000 + n * 37_001)
        spent += sum(c["charge_micro"] for c in tick(db, cap=8).result["charges"] if c["bid_id"] == "J")
        assert n < 100
    assert status(db, "J") == Status.COMPLETED and spent <= escrow


def test_tick_skipped_when_capacity_service_is_down(db):
    submit(db, "A", 1, qty=8, price=1_000_000)
    tick(db)

    async def down():
        raise ConnectionError("capacity down")

    class _CM:  # tick_once uses `with session_factory() as db`
        def __enter__(self): return db
        def __exit__(self, *a): return False
    assert asyncio.run(tick_once(lambda: _CM(), down, TICK_S)) == []
    assert status(db, "A") == Status.RUNNING  # NOT preempted by a bogus capacity of 0
    assert db.query(Tick).count() == 1  # only the manual tick; the skipped one wrote nothing


def test_http_endpoints():
    with TestClient(create_market_app("sqlite://")) as c:
        sf = c.app.state.session_factory
        with sf() as s:
            submit(s, "A", 1, qty=8, price=1_000_000)
        assert c.get("/v1/markets/A100/price").json()["tick_no"] == 0
        assert c.post("/v1/admin/tick/A100", params={"capacity": 8}).status_code == 200
        p = c.get("/v1/markets/A100/price").json()
        assert p["tick_no"] == 1 and p["filled_qty"] == 8
        assert c.get("/v1/markets/A100/ticks").json()[0]["tick_no"] == 1
        assert c.get("/v1/markets/A100/book").json() == [{"price_micro": 1_000_000, "status": "RUNNING", "qty": 8}]
        assert c.get("/v1/allocations", params={"user_id": "u1"}).json()[0]["status"] == "RUNNING"
        assert c.get("/v1/markets/NOPE/price").status_code == 404


def test_end_to_end_bid_event_relay_market_wallet():
    """bid svc -> outbox -> relay -> bus -> market projection -> tick -> market.cleared -> wallet capture."""
    from bid_svc.main import create_app as create_bid_app
    from bid_svc.wallet_client import WalletClient
    from wallet_svc.main import create_app as create_wallet_app

    with TestClient(create_wallet_app("sqlite://")) as wallet, \
            TestClient(create_bid_app("sqlite://", wallet=WalletClient(wallet))) as bids:
        wallet.post("/v1/wallets/alice/deposit", json={"amount_micro": 50_000_000, "idempotency_key": "d"})
        r = bids.post("/v1/bids", json={"user_id": "alice", "gpu_type": "A100", "qty": 4,
                                        "max_price_micro": 2_000_000, "duration_s": 3600})
        assert r.json()["status"] == "ACTIVE"

        bus = InMemoryBus()
        assert asyncio.run(relay_once(bids.app.state.session_factory, bus, "bid")) == 1
        assert asyncio.run(relay_once(bids.app.state.session_factory, bus, "bid")) == 0  # nothing re-sent

        e = make_engine("sqlite://")
        Base.metadata.create_all(e)
        with make_session_factory(e)() as mdb:
            for topic, _key, payload in bus.log:
                apply_event(mdb, topic, payload)
            mdb.commit()
            cleared = tick(mdb, cap=8).result
            charge = cleared["charges"][0]
            assert charge["user_id"] == "alice" and charge["qty"] == 4
            assert cleared["clearing_price_micro"] == A100

        # What Phase 4's settlement worker will do: capture using the event's idempotency key (twice = once).
        url = f"/v1/holds/{charge['bid_id']}/capture"
        body = {"amount_micro": charge["charge_micro"], "idempotency_key": charge["charge_key"]}
        assert wallet.post(url, json=body).json()["replayed"] is False
        assert wallet.post(url, json=body).json()["replayed"] is True
        w = wallet.get("/v1/wallets/alice").json()
        assert w["available_micro"] + w["held_micro"] == 50_000_000 - charge["charge_micro"]
        assert wallet.get("/v1/admin/integrity").json()["ok"] is True


def test_websocket_pushes_snapshot_then_each_new_tick(monkeypatch):
    monkeypatch.setenv("WS_POLL_S", "0.05")
    with TestClient(create_market_app("sqlite://")) as c:
        with c.websocket_connect("/ws/markets") as ws:
            snap = ws.receive_json()
            assert snap["type"] == "snapshot" and snap["markets"] == {} and snap["tick_seconds"] == 5
            c.post("/v1/admin/tick/A100", params={"capacity": 8})
            msg = ws.receive_json()
            assert msg["type"] == "tick" and msg["gpu_type"] == "A100" and msg["tick_no"] == 1
            assert msg["clearing_price_micro"] == RESERVE_MICRO["A100"]
        with c.websocket_connect("/ws/markets") as ws2:  # late joiner gets current state immediately
            assert ws2.receive_json()["markets"]["A100"]["tick_no"] == 1
