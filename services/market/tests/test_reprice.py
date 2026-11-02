"""Raising a bid: projection rules, rescue from a preemption notice, and one notice per bid."""
import asyncio

import pytest
from bid_svc.main import create_app as create_bid_app
from bid_svc.wallet_client import WalletClient
from fastapi.testclient import TestClient
from market_svc.events import apply_event
from market_svc.models import BookBid, Status
from market_svc.tick import run_tick
from spotcommon.config import RESERVE_MICRO
from spotcommon.db import Base, make_engine, make_session_factory
from spotcommon.events import InMemoryBus
from spotcommon.money import hold_amount
from spotcommon.outbox import Outbox, relay_once
from sqlalchemy import select
from wallet_svc.main import create_app as create_wallet_app

A100 = RESERVE_MICRO["A100"]
TICK_S, GRACE = 5, 15  # notice lasts 3 ticks


@pytest.fixture()
def db():
    e = make_engine("sqlite://")
    Base.metadata.create_all(e)
    with make_session_factory(e)() as s:
        yield s


def bid_payload(bid_id, seq, qty, price, version=0, dur=3600):
    return {"bid_id": bid_id, "seq": seq, "user_id": "u", "gpu_type": "A100", "qty": qty, "max_price_micro": price,
            "duration_s": dur, "divisible": False, "price_version": version,
            "hold_amount_micro": hold_amount(price, qty, dur, TICK_S)}


def event(db, topic, *a, **kw):
    r = apply_event(db, topic, bid_payload(*a, **kw))
    db.commit()
    return r


def tick(db, cap=8):
    t = run_tick(db, "A100", cap, TICK_S, grace_s=GRACE)
    db.commit()
    return t


def get(db, bid_id):
    db.expire_all()
    return db.get(BookBid, bid_id)


def on_notice(db):
    """low is running, then a whale outbids it: low is on notice."""
    event(db, "bid.submitted", "low", 1, 8, A100 + 100_000)
    tick(db)
    event(db, "bid.submitted", "whale", 2, 8, 5_000_000)
    tick(db)
    assert get(db, "low").grace_until_tick is not None


# ---------- projection ----------
def test_reprice_updates_price_and_ignores_stale_or_duplicate_versions(db):
    event(db, "bid.submitted", "b", 1, 2, 1_000_000)
    assert event(db, "bid.repriced", "b", 1, 2, 3_000_000, version=2) == "repriced"
    assert event(db, "bid.repriced", "b", 1, 2, 2_000_000, version=1) == "ignored"  # older raise arrives late
    assert event(db, "bid.repriced", "b", 1, 2, 3_000_000, version=2) == "ignored"  # duplicate delivery
    b = get(db, "b")
    assert b.max_price_micro == 3_000_000 and b.price_version == 2
    assert b.hold_amount_micro == hold_amount(3_000_000, 2, 3600, TICK_S)


def test_reprice_that_overtakes_the_submit_still_creates_the_bid_once(db):
    assert event(db, "bid.repriced", "b", 1, 2, 3_000_000, version=1) == "queued"
    assert event(db, "bid.submitted", "b", 1, 2, 1_000_000) == "ignored"  # the late original must not undo the raise
    assert get(db, "b").max_price_micro == 3_000_000


def test_reprice_after_cancel_is_ignored(db):
    event(db, "bid.submitted", "b", 1, 2, 1_000_000)
    apply_event(db, "bid.cancelled", {"bid_id": "b"})
    db.commit()
    assert event(db, "bid.repriced", "b", 1, 2, 3_000_000, version=1) == "ignored"
    assert get(db, "b").status == Status.CANCELLED


def test_queued_bid_can_be_raised_to_jump_the_queue(db):
    event(db, "bid.submitted", "first", 1, 8, 2_000_000)
    event(db, "bid.submitted", "waiting", 2, 8, 1_000_000)
    tick(db)
    assert get(db, "waiting").status == Status.QUEUED
    event(db, "bid.repriced", "waiting", 2, 8, 9_000_000, version=1)
    tick(db)  # first is incumbent but now outbid -> notice; waiting stays queued until the GPUs free up
    assert get(db, "first").grace_until_tick is not None
    for _ in range(3):
        tick(db)
    assert get(db, "waiting").status == Status.RUNNING and get(db, "first").status == Status.PREEMPTED


# ---------- rescue ----------
def test_raising_during_notice_cancels_it_and_the_job_competes_again_and_is_billed(db):
    on_notice(db)
    t = tick(db)
    assert t.result["charges"] == []  # still free while on notice
    assert event(db, "bid.repriced", "low", 1, 8, 9_000_000, version=1) == "repriced_cleared_notice"
    low = get(db, "low")
    assert low.grace_until_tick is None and low.grace_deadline_at is None and low.status == Status.RUNNING
    t = tick(db)
    assert get(db, "low").status == Status.RUNNING and get(db, "whale").status == Status.QUEUED
    assert [c["bid_id"] for c in t.result["charges"]] == ["low"]  # billed again, at the clearing price
    assert t.result["clearing_price_micro"] == 5_000_000  # the whale's losing bid sets the price
    assert t.result["preempted"] == [] and t.result["warned"] == []


def test_raise_that_still_loses_does_not_buy_a_second_notice(db):
    on_notice(db)
    event(db, "bid.repriced", "low", 1, 8, 4_000_000, version=1)  # rescued...
    t = tick(db)
    assert get(db, "low").status == Status.PREEMPTED  # ...but 4.0 < 5.0: out at once, no new free window
    assert t.result["preempted"] == ["low"] and t.result["warned"] == []
    assert get(db, "whale").status == Status.RUNNING


def test_rescued_job_outbid_again_is_preempted_immediately(db):
    on_notice(db)
    event(db, "bid.repriced", "low", 1, 8, 9_000_000, version=1)
    tick(db)
    event(db, "bid.repriced", "whale", 2, 8, 20_000_000, version=1)
    t = tick(db)
    assert get(db, "low").status == Status.PREEMPTED and t.result["warned"] == []
    assert get(db, "whale").status == Status.RUNNING


def test_never_more_gpus_running_than_capacity_through_the_whole_dance(db):
    on_notice(db)
    for step in range(6):
        if step == 2:
            event(db, "bid.repriced", "low", 1, 8, 9_000_000, version=1)
        tick(db)
        db.expire_all()
        assert sum(b.alloc_qty for b in db.scalars(select(BookBid)) if b.status == Status.RUNNING) <= 8


# ---------- end to end through the real bid + wallet services ----------
def test_end_to_end_raise_via_bid_api_rescues_a_job_on_notice(db):
    with TestClient(create_wallet_app("sqlite://")) as wallet, \
            TestClient(create_bid_app("sqlite://", wallet=WalletClient(wallet))) as bids:
        wallet.post("/v1/wallets/alice/deposit", json={"amount_micro": 500_000_000, "idempotency_key": "d"})
        r = bids.post("/v1/bids", json={"user_id": "alice", "gpu_type": "A100", "qty": 8,
                                        "max_price_micro": A100 + 100_000, "duration_s": 3600}).json()
        bid_id = r["bid_id"]

        bus, sf = InMemoryBus(), bids.app.state.session_factory

        def pump():
            """bid outbox -> relay -> market projection (in order)."""
            start = len(bus.log)
            asyncio.run(relay_once(sf, bus, "bid"))
            for topic, _k, payload in bus.log[start:]:
                apply_event(db, topic, payload)
            db.commit()

        pump()
        tick(db)
        assert get(db, bid_id).status == Status.RUNNING
        event(db, "bid.submitted", "whale", 99, 8, 5_000_000)
        tick(db)
        assert get(db, bid_id).grace_until_tick is not None

        before = wallet.get(f"/v1/holds/{bid_id}").json()["amount_micro"]
        assert bids.patch(f"/v1/bids/{bid_id}", json={"max_price_micro": 9_000_000}).status_code == 200
        assert wallet.get(f"/v1/holds/{bid_id}").json()["amount_micro"] > before
        pump()
        assert get(db, bid_id).grace_until_tick is None and get(db, bid_id).max_price_micro == 9_000_000
        t = tick(db)
        assert get(db, bid_id).status == Status.RUNNING and get(db, "whale").status == Status.QUEUED
        charge = t.result["charges"][0]
        assert charge["bid_id"] == bid_id
        # the settlement path captures against the grown hold, and the books still balance
        body = {"amount_micro": charge["charge_micro"], "idempotency_key": charge["charge_key"]}
        assert wallet.post(f"/v1/holds/{bid_id}/capture", json=body).status_code == 200
        assert wallet.get("/v1/admin/integrity").json()["ok"] is True
        with sf() as bdb:
            assert [o.topic for o in bdb.scalars(select(Outbox).order_by(Outbox.id))] == ["bid.submitted", "bid.repriced"]
