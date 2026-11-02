from __future__ import annotations

from datetime import datetime, timezone

from spotcommon.db import Base
from spotcommon.outbox import Outbox  # noqa: F401  (re-exported)
from sqlalchemy import BigInteger, Boolean, CheckConstraint, DateTime, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

PK_TYPE = BigInteger().with_variant(Integer, "sqlite")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class BidStatus:
    PENDING = "PENDING"      # accepted, escrow not confirmed yet
    ACTIVE = "ACTIVE"        # escrow held, eligible for the next tick
    REJECTED = "REJECTED"    # escrow refused (insufficient funds)
    CANCELLED = "CANCELLED"  # cancelled by user, escrow released
    RUNNING = "RUNNING"      # market gave it GPUs (from allocation.created)
    PREEMPTED = "PREEMPTED"  # outbid / capacity lost (from allocation.preempted)
    COMPLETED = "COMPLETED"  # ran its full duration (from allocation.completed)

    TERMINAL = (REJECTED, CANCELLED, PREEMPTED, COMPLETED)


class Bid(Base):
    __tablename__ = "bids"

    seq: Mapped[int] = mapped_column(PK_TYPE, primary_key=True, autoincrement=True)  # arrival order
    bid_id: Mapped[str] = mapped_column(String(64), unique=True)
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    gpu_type: Mapped[str] = mapped_column(String(16))
    qty: Mapped[int] = mapped_column(Integer)
    max_price_micro: Mapped[int] = mapped_column(BigInteger)  # per GPU-hour
    duration_s: Mapped[int] = mapped_column(Integer)
    divisible: Mapped[bool] = mapped_column(Boolean, default=False)
    checkpointable: Mapped[bool] = mapped_column(Boolean, default=False)
    priority: Mapped[int] = mapped_column(Integer, default=0)
    hold_amount_micro: Mapped[int] = mapped_column(BigInteger)
    status: Mapped[str] = mapped_column(String(12), default=BidStatus.PENDING)
    price_version: Mapped[int] = mapped_column(Integer, default=0)  # +1 each time the max price is raised
    reject_reason: Mapped[str | None] = mapped_column(String(120))
    client_request_id: Mapped[str | None] = mapped_column(String(80))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    __table_args__ = (
        CheckConstraint("qty > 0", name="ck_bid_qty"),
        CheckConstraint("max_price_micro >= 0", name="ck_bid_price"),
        Index("ix_bids_market", "gpu_type", "status"),
        Index("uq_bids_client_req", "user_id", "client_request_id", unique=True),
    )
