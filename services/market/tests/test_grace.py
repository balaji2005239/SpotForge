"""30-second preemption notice: outbid jobs keep their GPUs (unbilled) until the deadline, never double-booked."""
import pytest
from market_svc.events import apply_event
from market_svc.models import BookBid, Status
from market_svc.tick import run_tick
from spotcommon.config import RESERVE_MICRO
from spotcommon.db import Base, make_engine, make_session_factory
from spotcommon.money import hold_amount
from spotcommon.outbox import Outbox
from sqlalchemy import select

A100 = RESERVE_MICRO["A100"]
TICK_S = 5
GRACE = 15  # 3 ticks


@pytest.fixture()
def db():
    e = make_engine("sqlite://")
    Base.metadata.create_all(e)
    with make_session_factory(e)() as s:
        yield s


def submit(db, bid_id, seq, qty, price, dur=3600, divisible=False, user="u1"):
    apply_event(db, "bid.submitted", {
        "bid_id": bid_id, "seq": seq, "user_id": user, "gpu_type": "A100", "qty": qty,
        "max_price_micro": price, "duration_s": dur, "divisible": divisible,
        "hold_amount_micro": hold_amount(price, qty, dur, TICK_S)})
    db.commit()


def tick(db, cap=8, grace=GRACE):
    t = run_tick(db, "A100", cap, TICK_S, grace_s=grace)
    db.commit()
    return t


def get(db, bid_id):
    db.expire_all()
    return db.get(BookBid, bid_id)


def events(db):
    return [(o.topic, o.payload.get("bid_id")) for o in db.scalars(select(Outbox).order_by(Outbox.id))
            if o.topic.startswith("allocation.")]


def setup_outbid(db):
    submit(db, "low", 1, 8, A100 + 100_000)
    tick(db)  # low runs, filling all 8
    submit(db, "whale", 2, 8, 5_000_000)
    return tick(db)  # whale outbids low


def test_outbid_job_gets_notice_not_killed(db):
    t = setup_outbid(db)
    low, whale = get(db, "low"), get(db, "whale")
    assert low.status == Status.RUNNING and low.alloc_qty == 8  # still holds its GPUs
    assert low.grace_until_tick == 2 + 3 and low.grace_deadline_at is not None
    assert t.result["warned"] == ["low"] and t.result["preempted"] == []
    assert ("allocation.warned", "low") in events(db)
    assert whale.status == Status.QUEUED  # GPUs are not free yet: never double-booked


def test_notice_is_unbilled_then_job_is_preempted_and_winner_starts(db):
    setup_outbid(db)
    for _ in range(2):  # ticks 3, 4: still on notice
        t = tick(db)
        assert t.result["charges"] == [] and get(db, "low").status == Status.RUNNING
        assert get(db, "whale").status == Status.QUEUED
    t = tick(db)  # tick 5 = deadline
    assert get(db, "low").status == Status.PREEMPTED and t.result["preempted"] == ["low"]
    assert get(db, "whale").status == Status.RUNNING
    assert [c["bid_id"] for c in t.result["charges"]] == ["whale"]
    reasons = [o.payload.get("reason") for o in db.scalars(select(Outbox)) if o.topic == "allocation.preempted"]
    assert reasons == ["grace_expired"]


def test_grace_zero_keeps_old_immediate_preemption(db):
    submit(db, "low", 1, 8, A100 + 100_000)
    tick(db, grace=0)
    submit(db, "whale", 2, 8, 5_000_000)
    t = tick(db, grace=0)
    assert get(db, "low").status == Status.PREEMPTED and get(db, "whale").status == Status.RUNNING
    assert t.result["warned"] == []


def test_capacity_loss_skips_the_notice(db):
    submit(db, "A", 1, 8, 2_000_000)
    tick(db)
    t = tick(db, cap=0)  # every node died: nothing to hold on to
    assert get(db, "A").status == Status.PREEMPTED and t.result["warned"] == []


def test_capacity_loss_while_on_notice_drops_warned_job_first(db):
    setup_outbid(db)
    t = tick(db, cap=0)
    assert get(db, "low").status == Status.PREEMPTED
    assert t.result["preempted"] == ["low"]
    reasons = [o.payload.get("reason") for o in db.scalars(select(Outbox)) if o.topic == "allocation.preempted"]
    assert reasons == ["capacity_lost"]


def test_physical_gpus_never_exceed_capacity_during_grace(db):
    setup_outbid(db)
    for _ in range(5):
        db.expire_all()
        held = sum(b.alloc_qty for b in db.scalars(select(BookBid)) if b.status == Status.RUNNING)
        assert held <= 8
        tick(db)


def test_partial_outbid_warns_only_the_loser_and_new_winner_fits_remaining_gpus(db):
    submit(db, "keep", 1, 4, 3_000_000)
    submit(db, "weak", 2, 4, A100 + 100_000)
    tick(db)
    submit(db, "mid", 3, 4, 2_000_000)  # outbids weak, but weak holds the other 4 GPUs for now
    t = tick(db)
    assert t.result["warned"] == ["weak"]
    assert get(db, "keep").status == Status.RUNNING and get(db, "keep").grace_until_tick is None
    assert get(db, "mid").status == Status.QUEUED
    for _ in range(3):
        tick(db)
    assert get(db, "weak").status == Status.PREEMPTED and get(db, "mid").status == Status.RUNNING


def test_cancel_during_notice_stops_it(db):
    setup_outbid(db)
    apply_event(db, "bid.cancelled", {"bid_id": "low"})
    db.commit()
    t = tick(db)
    assert get(db, "low").status == Status.CANCELLED
    assert get(db, "whale").status == Status.RUNNING and t.result["warned"] == []
