"""Transactional outbox + relay.

Services write events to the `outbox` table in the SAME transaction as their state change.
A relay publishes unpublished rows to the bus and marks them. Delivery is at-least-once
(a crash between publish and commit re-sends), so every consumer must be idempotent.
Run exactly one relay per service so per-key ordering is preserved.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from sqlalchemy import JSON, DateTime, Index, String, select
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker

from .db import PK_TYPE, Base
from .events import EventBus


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Outbox(Base):
    __tablename__ = "outbox"

    id: Mapped[int] = mapped_column(PK_TYPE, primary_key=True, autoincrement=True)
    topic: Mapped[str] = mapped_column(String(64))
    key: Mapped[str] = mapped_column(String(64))  # Kafka partition key (gpu_type)
    payload: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (Index("ix_outbox_unpublished", "published_at", "id"),)


def emit(db, topic: str, key: str, payload: dict) -> None:
    db.add(Outbox(topic=topic, key=key, payload=payload))


async def relay_once(session_factory: sessionmaker, bus: EventBus, source: str, batch: int = 200) -> int:
    """Blocking DB calls run in worker threads: a coroutine that waits on a database lock must never stall the
    event loop, because the lock holder may itself need the loop to finish (found the hard way on sqlite)."""
    db = session_factory()
    try:
        rows = await asyncio.to_thread(
            lambda: db.scalars(select(Outbox).where(Outbox.published_at.is_(None)).order_by(Outbox.id)
                               .limit(batch).with_for_update(skip_locked=True)).all())
        for r in rows:
            await bus.publish(r.topic, r.key, {**r.payload, "event_id": f"{source}:{r.id}"})
            r.published_at = _now()
        await asyncio.to_thread(db.commit)
        return len(rows)
    finally:
        await asyncio.to_thread(db.close)


async def run_relay(session_factory: sessionmaker, bus: EventBus, source: str,
                    interval: float = 0.2, stop: asyncio.Event | None = None) -> None:
    stop = stop or asyncio.Event()
    while not stop.is_set():
        try:
            n = await relay_once(session_factory, bus, source)
        except Exception:  # broker down etc: back off and retry, never lose rows
            n = 0
            await asyncio.sleep(1.0)
        if n == 0:
            await asyncio.sleep(interval)
