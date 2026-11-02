"""Keeps bid.status in sync with what the market did (allocation.* events). Idempotent and
order-tolerant: events from different topics may arrive in any order, so terminal states stick."""
from __future__ import annotations

import asyncio

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from .models import Bid, BidStatus

_TARGET = {
    "allocation.created": BidStatus.RUNNING,
    "allocation.preempted": BidStatus.PREEMPTED,
    "allocation.completed": BidStatus.COMPLETED,
}


def apply_allocation_event(db: Session, topic: str, payload: dict) -> str:
    target = _TARGET.get(topic)
    if target is None:
        return "ignored"
    bid = db.scalar(select(Bid).where(Bid.bid_id == payload["bid_id"]))
    if bid is None:
        return "unknown_bid"
    if bid.status in BidStatus.TERMINAL:  # e.g. a late "created" after "completed", or a cancelled bid
        return "ignored"
    if target == BidStatus.RUNNING and bid.status != BidStatus.ACTIVE:
        return "ignored"
    bid.status = target
    return "updated"


async def consume_allocations(session_factory: sessionmaker, bootstrap: str) -> None:
    from spotcommon.events import consume

    topics = list(_TARGET)
    async for topic, payload, commit in consume(bootstrap, "bid-projection", topics):
        def apply(topic=topic, payload=payload) -> None:
            with session_factory() as db:
                apply_allocation_event(db, topic, payload)
                db.commit()

        await asyncio.to_thread(apply)
        await commit()
