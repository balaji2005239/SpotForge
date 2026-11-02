from __future__ import annotations

from datetime import datetime, timezone

import sqlalchemy as sa
from spotcommon.db import PK_TYPE, Base
from spotcommon.outbox import Outbox  # noqa: F401  (table lives in this service's DB too)
from sqlalchemy import JSON, BigInteger, Boolean, DateTime, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Status:
    QUEUED = "QUEUED"        # in the book, competing each tick
    RUNNING = "RUNNING"      # holds GPUs; re-enters every tick
    PREEMPTED = "PREEMPTED"  # outbid / capacity lost (terminal in this phase)
    COMPLETED = "COMPLETED"  # ran for its full duration
    CANCELLED = "CANCELLED"

    LIVE = (QUEUED, RUNNING)


class BookBid(Base):
    """This service's own projection of bids (built from bid.* events; never reads the bid DB)."""

    __tablename__ = "book_bids"

    bid_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    seq: Mapped[int] = mapped_column(BigInteger)  # arrival order, assigned by the bid service
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    gpu_type: Mapped[str] = mapped_column(String(16))
    qty: Mapped[int] = mapped_column(Integer)
    max_price_micro: Mapped[int] = mapped_column(BigInteger)
    duration_s: Mapped[int] = mapped_column(Integer)
    divisible: Mapped[bool] = mapped_column(Boolean, default=False)
    hold_amount_micro: Mapped[int] = mapped_column(BigInteger, default=0)
    status: Mapped[str] = mapped_column(String(10), default=Status.QUEUED)
    alloc_qty: Mapped[int] = mapped_column(Integer, default=0)
    ticks_run: Mapped[int] = mapped_column(Integer, default=0)
    price_version: Mapped[int] = mapped_column(Integer, default=0)  # latest bid.repriced applied (stale ones ignored)
    notice_used: Mapped[bool] = mapped_column(Boolean, default=False)  # a bid gets ONE preemption notice, ever
    # A cancelled bid is announced exactly once, in a tick's market.cleared (after its last charge): settlement
    # then releases the escrow. False until that tick.
    cancel_announced: Mapped[bool] = mapped_column(Boolean, default=False, server_default=sa.false())
    # Preemption notice: set when the job lost the auction but keeps its GPUs (unbilled) until this tick.
    grace_until_tick: Mapped[int | None] = mapped_column(Integer, nullable=True)
    grace_deadline_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)

    __table_args__ = (Index("ix_book_market", "gpu_type", "status"),)


class Tick(Base):
    __tablename__ = "ticks"

    id: Mapped[int] = mapped_column(PK_TYPE, primary_key=True, autoincrement=True)
    gpu_type: Mapped[str] = mapped_column(String(16))
    tick_no: Mapped[int] = mapped_column(Integer)
    snapshot_id: Mapped[str] = mapped_column(String(40), unique=True)
    epoch: Mapped[int | None] = mapped_column(Integer, nullable=True)  # fencing token of the leader that wrote it
    digest: Mapped[str] = mapped_column(String(64))
    clearing_price_micro: Mapped[int] = mapped_column(BigInteger)
    capacity: Mapped[int] = mapped_column(Integer)
    filled_qty: Mapped[int] = mapped_column(Integer)
    result: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    # One writer per (market, tick number): a second concurrent/duplicate tick is rejected by the DB.
    __table_args__ = (UniqueConstraint("gpu_type", "tick_no", name="uq_tick_market_no"),)
