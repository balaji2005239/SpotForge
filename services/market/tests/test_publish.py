"""Ticks are fanned out to Redis only after they commit, only by the leader, and never break the tick."""
import asyncio
import json

import fakeredis
import pytest
from market_svc.events import apply_event
from market_svc.lease import Fence
from market_svc.models import Tick
from market_svc.publish import CHANNEL, TickPublisher
from market_svc.worker import tick_once, ticker_step
from spotcommon.db import Base, make_engine, make_session_factory
from spotcommon.money import hold_amount
from sqlalchemy import func, select


@pytest.fixture()
def sf(tmp_path):
    e = make_engine(f"sqlite:///{tmp_path}/market.db")
    Base.metadata.create_all(e)
    return make_session_factory(e)


async def caps():
    return {"A100": {"gpus": 8}, "T4": {"gpus": 8}, "H100": {"gpus": 8}}


def collect(sf):
    seen, seen_in_db = [], []

    def publish(msg):
        seen.append(msg)
        with sf() as db:  # at publish time the tick must already be durable
            seen_in_db.append(db.scalar(select(func.count()).select_from(Tick)))
    return seen, seen_in_db, publish


def test_each_committed_tick_is_published_after_commit(sf):
    seen, in_db, publish = collect(sf)
    asyncio.run(tick_once(sf, caps, 5, 0, publish=publish))
    assert sorted(m["gpu_type"] for m in seen) == ["A100", "H100", "T4"]
    assert in_db == [1, 2, 3]  # n-th publish saw n committed ticks


def test_message_matches_what_the_api_serves_and_carries_notice_fields(sf):
    with sf() as db:
        apply_event(db, "bid.submitted", {"bid_id": "b", "seq": 1, "user_id": "u", "gpu_type": "A100", "qty": 2,
                                          "max_price_micro": 2_000_000, "duration_s": 600, "divisible": False,
                                          "hold_amount_micro": hold_amount(2_000_000, 2, 600, 5)})
        db.commit()
    seen, _, publish = collect(sf)
    asyncio.run(tick_once(sf, caps, 5, 30, publish=publish))
    m = next(x for x in seen if x["gpu_type"] == "A100")
    assert m["tick_no"] == 1 and m["warned"] == [] and m["grace_seconds"] == 30 and m["tick_seconds"] == 5
    assert m["clearing_price_micro"] == 800_000


def test_redis_down_does_not_fail_the_tick(sf):
    class Dead:
        def publish(self, *a):
            raise ConnectionError("redis down")

    done = asyncio.run(tick_once(sf, caps, 5, 0, publish=TickPublisher(Dead())))
    assert len(done) == 3
    with sf() as db:
        assert db.scalar(select(func.count()).select_from(Tick)) == 3


def test_publisher_writes_json_to_the_channel(sf):
    r = fakeredis.FakeRedis(decode_responses=True)
    ps = r.pubsub()
    ps.subscribe(CHANNEL)
    ps.get_message(timeout=1)
    asyncio.run(tick_once(sf, caps, 5, 0, publish=TickPublisher(r)))
    got = []
    while (m := ps.get_message(timeout=0.2)):
        got.append(json.loads(m["data"]))
    assert len(got) == 3 and all("tick_no" in g for g in got)


def test_standby_and_fenced_replicas_publish_nothing(sf):
    seen, _, publish = collect(sf)
    asyncio.run(ticker_step(sf, caps, 5, 0, "A", 10, publish=publish))
    n = len(seen)
    assert asyncio.run(ticker_step(sf, caps, 5, 0, "B", 10, publish=publish)) is None  # standby
    assert asyncio.run(tick_once(sf, caps, 5, 0, Fence("B", 7), publish=publish)) == []  # fenced
    assert len(seen) == n
