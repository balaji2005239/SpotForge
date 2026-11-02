"""Seeded chaos harness: the whole money loop in one process, with the failure modes a real deployment has.

    bid API --(outbox)--> bus --> market (ticks) --(outbox)--> bus --> bid projection + settlement --> wallet

What gets injected (all from one seeded RNG, so a failing seed replays exactly):
  * wallet calls from the bid service AND from settlement lose the request (never arrived) or the response
    (it DID execute, the caller sees a timeout) -- the nasty one for money
  * events are delivered at-least-once: duplicated, replayed from history, and interleaved across topics
    (per-topic order is kept; there is no ordering between topics, exactly like Kafka)
  * node failures: per-GPU capacity randomly drops (even to zero) and recovers
  * clients retry submits with the same client_request_id, cancel, and raise bids at random

After a wind-down (faults off, everything drained) the invariants in `check_final` must hold.
"""
from __future__ import annotations

import asyncio
import random
from collections import Counter, deque
from contextlib import ExitStack, contextmanager
from datetime import datetime, timedelta, timezone

import httpx
from bid_svc.main import create_app as create_bid_app
from bid_svc.models import Bid, BidStatus
from bid_svc.projection import apply_allocation_event
from bid_svc.wallet_client import WalletClient
from fastapi.testclient import TestClient
from market_svc.events import apply_event
from market_svc.models import BookBid, Status, Tick
from market_svc.tick import run_tick
from settlement_svc.ingest import ingest_cleared
from settlement_svc.models import Item, Kind, State
from settlement_svc.processor import process_pending, reconcile
from settlement_svc.wallet_client import SettlementWallet
from spotcommon.config import GPU_TYPES, RESERVE_MICRO
from spotcommon.db import Base, make_engine, make_session_factory
from spotcommon.events import InMemoryBus
from spotcommon.outbox import relay_once
from sqlalchemy import func, select
from wallet_svc.main import create_app as create_wallet_app

TICK_S = 5
BASE_CAPACITY = {"T4": 16, "A100": 16, "H100": 8}
DURATIONS = (30, 60, 120, 300, 3600)
PRICE_MULT = (1.0, 1.1, 1.5, 2.0, 3.0, 5.0)


class Flaky:
    """Wraps an httpx-like client. Per call: lose the request, or run it and lose the response."""

    def __init__(self, inner, rng: random.Random, p_lost_request: float, p_lost_response: float):
        self.inner, self.rng = inner, rng
        self.p_req, self.p_resp = p_lost_request, p_lost_response
        self.enabled = True
        self.lost_requests = self.lost_responses = self.calls = 0

    def _call(self, name: str, *a, **kw):
        fn = getattr(self.inner, name)
        self.calls += 1
        if not self.enabled:
            return fn(*a, **kw)
        r = self.rng.random()
        if r < self.p_req:
            self.lost_requests += 1
            raise httpx.ConnectError("injected: request lost")
        resp = fn(*a, **kw)
        if r < self.p_req + self.p_resp:
            self.lost_responses += 1
            raise httpx.ReadTimeout("injected: response lost")
        return resp

    def post(self, *a, **kw):
        return self._call("post", *a, **kw)

    def get(self, *a, **kw):
        return self._call("get", *a, **kw)


class Pipeline:
    def __init__(self, stack: ExitStack, seed: int, users: int = 6, funds: int = 30_000_000,
                 p_lost_request: float = 0.10, p_lost_response: float = 0.10,
                 p_dup: float = 0.12, p_replay: float = 0.05, grace_s: int = 15,
                 pump_range: tuple[int, int] = (2, 8)):
        self.rng = random.Random(seed)
        self.p_dup, self.p_replay, self.grace_s = p_dup, p_replay, grace_s
        self.pump_range = pump_range
        self.users = [f"u{i}" for i in range(users)]
        self.funds = funds
        self.now = datetime.now(timezone.utc)

        self.wallet = stack.enter_context(TestClient(create_wallet_app("sqlite://")))
        for u in self.users:
            self.wallet.post(f"/v1/wallets/{u}/deposit", json={"amount_micro": funds, "idempotency_key": f"dep-{u}"})
        self.bid_wallet = Flaky(self.wallet, self.rng, p_lost_request, p_lost_response)
        self.settle_wallet = Flaky(self.wallet, self.rng, p_lost_request, p_lost_response)
        self.api = stack.enter_context(
            TestClient(create_bid_app("sqlite://", wallet=WalletClient(self.bid_wallet))))
        self.bid_sf = self.api.app.state.session_factory
        self.market_sf = self._fresh_db()
        self.settle_sf = self._fresh_db()
        self.swallet = SettlementWallet(self.settle_wallet)

        self.bus = {"bid": InMemoryBus(), "market": InMemoryBus()}
        self.cursor = {"bid": 0, "market": 0}
        self.queues: dict[str, deque] = {}
        self.history: list[tuple[str, dict]] = []
        self.capacity = dict.fromkeys(GPU_TYPES, 0) | BASE_CAPACITY

        self.price: dict[str, int] = {}          # bid_id -> highest accepted max price
        self.owner: dict[str, str] = {}
        self.cancel_attempted: set[str] = set()  # cancelled by the client (their view may lag the market's)
        self.retry_submits: list[dict] = []
        self.n_requests = 0
        self.rounds = 0
        self.counts: Counter = Counter()

    @staticmethod
    def _fresh_db():
        e = make_engine("sqlite://")
        Base.metadata.create_all(e)
        return make_session_factory(e)

    # ---------------------------------------------------------------- client actions
    def _body(self) -> dict:
        gpu = self.rng.choice(GPU_TYPES)
        self.n_requests += 1
        return {"user_id": self.rng.choice(self.users), "gpu_type": gpu, "qty": self.rng.randint(1, 8),
                "max_price_micro": int(RESERVE_MICRO[gpu] * self.rng.choice(PRICE_MULT)),
                "duration_s": self.rng.choice(DURATIONS), "divisible": self.rng.random() < 0.3,
                "client_request_id": f"req-{self.n_requests}"}

    def _submit(self, body: dict) -> None:
        r = self.api.post("/v1/bids", json=body)
        self.counts[f"submit_{r.status_code}"] += 1
        if r.status_code == 503:
            if body not in self.retry_submits:
                self.retry_submits.append(body)
            return
        assert r.status_code in (201, 422), (r.status_code, r.text)
        if body in self.retry_submits:
            self.retry_submits.remove(body)
        if r.status_code == 201:
            j = r.json()
            self.owner[j["bid_id"]] = j["user_id"]
            self.price[j["bid_id"]] = j["max_price_micro"]

    def _cancel(self) -> None:
        live = [b for b in self.owner if b not in self.cancel_attempted]
        if not live:
            return
        bid_id = self.rng.choice(live)
        r = self.api.delete(f"/v1/bids/{bid_id}")
        self.counts[f"cancel_{r.status_code}"] += 1
        assert r.status_code in (200, 409), (r.status_code, r.text)  # cancel never touches the wallet: no 503
        if r.status_code == 200:
            self.cancel_attempted.add(bid_id)

    def _raise(self) -> None:
        if not self.price:
            return
        bid_id = self.rng.choice(list(self.price))
        new = int(self.price[bid_id] * self.rng.uniform(1.05, 2.5))
        r = self.api.patch(f"/v1/bids/{bid_id}", json={"max_price_micro": new})
        self.counts[f"raise_{r.status_code}"] += 1
        assert r.status_code in (200, 402, 404, 409, 422, 503), (r.status_code, r.text)
        if r.status_code == 200:
            self.price[bid_id] = max(self.price[bid_id], r.json()["max_price_micro"])

    # ---------------------------------------------------------------- the market
    def _chaos_capacity(self) -> None:
        for g in GPU_TYPES:
            if self.rng.random() < 0.08:
                self.capacity[g] = self.rng.choice((0, BASE_CAPACITY[g] // 2, BASE_CAPACITY[g], BASE_CAPACITY[g]))
                self.counts["capacity_changes"] += 1

    def tick_all(self) -> None:
        for g in GPU_TYPES:
            with self.market_sf() as db:
                t = run_tick(db, g, self.capacity[g], TICK_S, grace_s=self.grace_s)
                db.commit()
                assert t is not None
                running = db.scalar(select(func.coalesce(func.sum(BookBid.alloc_qty), 0)).where(
                    BookBid.gpu_type == g, BookBid.status == Status.RUNNING))
                assert running <= self.capacity[g], f"{g} overbooked after tick {t.tick_no}: {running}>{self.capacity[g]}"
                self.counts["warned"] += len(t.result["warned"])
                self.counts["preempted"] += len(t.result["preempted"])
                self.counts["completed"] += len(t.result["completed"])

    # ---------------------------------------------------------------- the bus
    def relay(self) -> None:
        asyncio.run(relay_once(self.bid_sf, self.bus["bid"], "bid"))
        asyncio.run(relay_once(self.market_sf, self.bus["market"], "market"))
        for name, bus in self.bus.items():
            for topic, _key, payload in bus.log[self.cursor[name]:]:
                self.queues.setdefault(topic, deque()).append(payload)
            self.cursor[name] = len(bus.log)

    def _deliver(self, topic: str, p: dict) -> None:
        if topic.startswith("bid."):
            with self.market_sf() as db:
                apply_event(db, topic, p)
                db.commit()
        elif topic.startswith("allocation."):
            with self.bid_sf() as db:
                apply_allocation_event(db, topic, p)
                db.commit()
        elif topic == "market.cleared":
            with self.settle_sf() as db:
                ingest_cleared(db, p)
                db.commit()

    def pump(self, n: int | None = None, chaos: bool = True) -> None:
        """Deliver up to n messages (None = until every queue is empty)."""
        delivered = 0
        while any(self.queues.values()) and (n is None or delivered < n):
            topic = self.rng.choice([t for t, q in self.queues.items() if q])
            p = self.queues[topic].popleft()
            self.history.append((topic, p))
            self._deliver(topic, p)
            delivered += 1
            if chaos and self.rng.random() < self.p_dup:
                self._deliver(topic, p)
                self.counts["dup_delivered"] += 1
            if chaos and self.history and self.rng.random() < self.p_replay:
                self._deliver(*self.rng.choice(self.history))
                self.counts["replayed"] += 1

    def settle(self, advance_s: float = 10) -> None:
        self.now += timedelta(seconds=advance_s)
        st = process_pending(self.settle_sf, self.swallet, now=self.now, max_attempts=12)
        self.counts["settle_retried"] += st.retried

    # ---------------------------------------------------------------- driving it
    def round(self) -> None:
        self.rounds += 1
        for body in list(self.retry_submits):
            self._submit(body)
        for _ in range(self.rng.randint(0, 3)):
            x = self.rng.random()
            if x < 0.55:
                self._submit(self._body())
            elif x < 0.70:
                self._cancel()
            else:
                self._raise()
        if self.rounds % 5 == 0:
            self.api.app.state.sweep_pending(0, now=self.now + timedelta(days=1))
        self._chaos_capacity()
        self.tick_all()
        self.relay()
        self.pump(self.rng.randint(*self.pump_range))
        self.settle()

    def faults(self, on: bool) -> None:
        self.bid_wallet.enabled = self.settle_wallet.enabled = on

    def drain(self) -> None:
        """Faults off; push every in-flight thing to the end."""
        self.faults(False)
        for body in list(self.retry_submits):
            self._submit(body)
        assert not self.retry_submits
        self.api.app.state.sweep_pending(0, now=self.now + timedelta(days=1))
        for _ in range(30):
            self.relay()
            self.pump(chaos=False)
            self.settle(60)
            with self.settle_sf() as db:
                pending = db.scalar(select(func.count()).select_from(Item).where(Item.state == State.PENDING))
            if not pending and not any(self.queues.values()):
                self.relay()
                if not any(self.queues.values()):
                    return
        raise AssertionError("pipeline did not quiesce")

    def cancel_everything_live(self) -> None:
        for u in self.users:
            for b in self.api.get("/v1/bids", params={"user_id": u}).json():
                if b["status"] in (BidStatus.ACTIVE, BidStatus.RUNNING):
                    r = self.api.delete(f"/v1/bids/{b['bid_id']}")
                    assert r.status_code == 200, r.text
                    self.cancel_attempted.add(b["bid_id"])

    # ---------------------------------------------------------------- invariants
    def check_projection(self) -> None:
        """Before anything is cancelled: the bid service's view must equal the market's view."""
        with self.bid_sf() as bdb, self.market_sf() as mdb:
            book = {b.bid_id: b for b in mdb.scalars(select(BookBid))}
            for b in bdb.scalars(select(Bid)):
                assert b.status != BidStatus.PENDING, f"{b.bid_id} stuck PENDING"
                if b.status == BidStatus.REJECTED or b.bid_id in self.cancel_attempted:
                    continue
                m = book.get(b.bid_id)
                assert m is not None, f"{b.bid_id} ({b.status}) never reached the market"
                want = {BidStatus.ACTIVE: Status.QUEUED}.get(b.status, b.status)
                assert m.status == want, f"{b.bid_id}: bid service {b.status} vs market {m.status}"
                if m.status in Status.LIVE:  # a raise that lands after the bid finished is ignored on purpose
                    assert m.price_version == b.price_version, f"{b.bid_id}: version {m.price_version}!={b.price_version}"
                    assert m.max_price_micro == b.max_price_micro, f"{b.bid_id}: price drifted"

    def check_final(self) -> dict:
        w = self.wallet
        assert w.get("/v1/admin/integrity").json()["ok"] is True, w.get("/v1/admin/integrity").json()

        total = captured = 0
        holds = {}
        for u in self.users:
            bal = w.get(f"/v1/wallets/{u}").json()
            assert bal["held_micro"] == 0, f"{u} still has {bal['held_micro']} in escrow"
            total += bal["available_micro"] + bal["held_micro"]
            for h in w.get(f"/v1/wallets/{u}/holds").json():
                assert h["status"] == "CLOSED", f"hold {h['hold_id']} left {h['status']}"
                holds[h["hold_id"]] = h
                captured += h["captured_micro"]
        assert total + captured == self.funds * len(self.users), "money was created or destroyed"

        with self.market_sf() as mdb:
            charged: Counter = Counter()
            ticks = mdb.scalars(select(Tick)).all()
            for t in ticks:
                for c in t.result["charges"]:
                    charged[c["bid_id"]] += c["charge_micro"]
            assert len({(t.gpu_type, t.tick_no) for t in ticks}) == len(ticks)
            live = mdb.scalar(select(func.count()).select_from(BookBid).where(BookBid.status.in_(Status.LIVE)))
            assert live == 0, f"{live} bids still live in the market"

        with self.settle_sf() as sdb:
            items = sdb.scalars(select(Item)).all()
            by_state = Counter(i.state for i in items)
            assert by_state[State.PENDING] == 0 and by_state[State.DEAD] == 0, by_state
            for i in items:
                if i.state == State.SKIPPED:
                    h = holds[i.bid_id]  # only legitimate case: escrow fully consumed by captures, nothing to release
                    assert i.kind == Kind.RELEASE and h["captured_micro"] == h["amount_micro"], \
                        f"{i.kind} for {i.bid_id} skipped: a charge was lost or a release was refused: {h}"
            assert reconcile(sdb, self.swallet) == []
            # every captured micro-dollar is accounted for exactly once
            done_cap: Counter = Counter()
            for i in items:
                if i.kind == Kind.CAPTURE and i.state == State.DONE:
                    done_cap[i.bid_id] += i.amount_micro
        for bid_id, owed in charged.items():  # EVERY bid, cancelled or not: not one micro-dollar lost or doubled
            got = holds[bid_id]["captured_micro"] if bid_id in holds else 0
            assert got == done_cap[bid_id], f"{bid_id}: wallet captured {got}, settlement says {done_cap[bid_id]}"
            assert got == owed, f"{bid_id}: market charged {owed}, wallet captured {got}"
        with self.bid_sf() as bdb:
            left = bdb.scalar(select(func.count()).select_from(Bid).where(
                Bid.status.in_((BidStatus.PENDING, BidStatus.ACTIVE, BidStatus.RUNNING))))
            assert left == 0, f"{left} bids not terminal in the bid service"
        return {"bids": len(self.owner), "ticks": len(ticks), "revenue_micro": captured, "items": dict(by_state),
                "charged_bids": len(charged), "wallet_lost_requests": self.bid_wallet.lost_requests
                + self.settle_wallet.lost_requests, "wallet_lost_responses": self.bid_wallet.lost_responses
                + self.settle_wallet.lost_responses, **self.counts}


@contextmanager
def pipeline(seed: int, **kw):
    with ExitStack() as stack:
        yield Pipeline(stack, seed, **kw)


def run_soak(seed: int, rounds: int = 100, **kw) -> dict:
    with pipeline(seed, **kw) as p:
        for _ in range(rounds):
            p.round()
        p.drain()
        p.check_projection()
        p.cancel_everything_live()
        p.drain()  # the cancel events reach the market ...
        p.tick_all()  # ... whose next tick announces them, and settlement refunds behind the last charge
        p.relay()
        p.drain()
        return p.check_final()
