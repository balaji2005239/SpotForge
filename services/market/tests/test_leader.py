"""Leader election + fencing for the ticker."""
import asyncio
from datetime import datetime, timedelta, timezone
from threading import Thread

import pytest
from market_svc.events import apply_event
from market_svc.lease import TICKER, Fence, Lease, holds_lease, try_acquire
from market_svc.models import Tick
from market_svc.worker import tick_once, ticker_step
from spotcommon.db import Base, make_engine, make_session_factory
from spotcommon.money import hold_amount
from sqlalchemy import func, select

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
LEASE = 10


@pytest.fixture()
def sf(tmp_path):
    e = make_engine(f"sqlite:///{tmp_path}/market.db")
    Base.metadata.create_all(e)
    return make_session_factory(e)


async def caps():
    return {"A100": {"gpus": 8}, "T4": {"gpus": 8}, "H100": {"gpus": 8}}


def acquire(sf, holder, at):
    with sf() as db:
        e = try_acquire(db, TICKER, holder, LEASE, T0 + timedelta(seconds=at))
        db.commit()
        return e


def ticks(sf):
    with sf() as db:
        return db.scalar(select(func.count()).select_from(Tick))


def test_first_replica_leads_and_second_stands_by(sf):
    assert acquire(sf, "A", 0) == 1
    assert acquire(sf, "B", 1) is None
    assert acquire(sf, "A", 5) == 1  # renewal keeps the same epoch


def test_takeover_after_expiry_bumps_epoch(sf):
    acquire(sf, "A", 0)
    assert acquire(sf, "B", LEASE - 1) is None
    assert acquire(sf, "B", LEASE + 1) == 2
    assert acquire(sf, "A", LEASE + 2) is None  # A is now the follower


def test_renewal_prevents_takeover(sf):
    acquire(sf, "A", 0)
    for t in (4, 8, 12, 16):
        assert acquire(sf, "A", t) == 1
        assert acquire(sf, "B", t + 1) is None


def test_standby_replica_does_not_tick(sf):
    t = T0
    assert asyncio.run(ticker_step(sf, caps, 5, 0, "A", LEASE, t)) is not None
    n = ticks(sf)
    assert n == 3  # one per market
    assert asyncio.run(ticker_step(sf, caps, 5, 0, "B", LEASE, t + timedelta(seconds=1))) is None
    assert ticks(sf) == n


def test_deposed_leader_is_fenced_and_writes_nothing(sf):
    asyncio.run(ticker_step(sf, caps, 5, 0, "A", LEASE, T0))  # A leads, epoch 1
    stale = Fence("A", 1)
    later = T0 + timedelta(seconds=LEASE + 1)
    assert asyncio.run(ticker_step(sf, caps, 5, 0, "B", LEASE, later)) is not None  # B takes over, epoch 2
    before = ticks(sf)
    # A wakes up from a long pause still believing it leads:
    assert asyncio.run(tick_once(sf, caps, 5, 0, stale, later)) == []
    assert ticks(sf) == before
    with sf() as db:
        assert not holds_lease(db, TICKER, stale, later)
        assert holds_lease(db, TICKER, Fence("B", 2), later)
        assert {t.epoch for t in db.scalars(select(Tick))} == {1, 2}  # each tick records who wrote it


def test_expired_leader_cannot_tick_even_if_nobody_took_over(sf):
    asyncio.run(ticker_step(sf, caps, 5, 0, "A", LEASE, T0))
    n = ticks(sf)
    assert asyncio.run(tick_once(sf, caps, 5, 0, Fence("A", 1), T0 + timedelta(seconds=LEASE + 5))) == []
    assert ticks(sf) == n


def test_racing_replicas_produce_exactly_one_writer_and_no_duplicate_ticks(sf):
    wrote: dict[str, int] = {}

    def replica(name):
        n = 0
        for _ in range(12):
            r = asyncio.run(ticker_step(sf, caps, 5, 0, name, 60))  # real clock, long lease
            n += len(r or [])
        wrote[name] = n

    threads = [Thread(target=replica, args=(f"r{i}",)) for i in range(4)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert sum(1 for n in wrote.values() if n) == 1  # exactly one replica ever ticked
    with sf() as db:
        per = db.execute(select(Tick.gpu_type, func.count(), func.count(func.distinct(Tick.tick_no)))
                         .group_by(Tick.gpu_type)).all()
        assert all(c == d == 12 for _, c, d in per)  # 12 steps -> 12 distinct ticks per market
        assert db.get(Lease, TICKER).epoch == 1


def test_bids_still_clear_under_election(sf):
    with sf() as db:
        apply_event(db, "bid.submitted", {
            "bid_id": "b1", "seq": 1, "user_id": "u", "gpu_type": "A100", "qty": 2, "max_price_micro": 2_000_000,
            "duration_s": 600, "divisible": False, "hold_amount_micro": hold_amount(2_000_000, 2, 600, 5)})
        db.commit()
    asyncio.run(ticker_step(sf, caps, 5, 0, "A", LEASE, T0))
    with sf() as db:
        from market_svc.models import BookBid
        assert db.get(BookBid, "b1").status == "RUNNING"
