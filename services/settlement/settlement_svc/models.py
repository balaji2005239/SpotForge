from __future__ import annotations

from datetime import datetime, timezone

from spotcommon.db import PK_TYPE, Base
from sqlalchemy import BigInteger, DateTime, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Kind:
    CAPTURE = "CAPTURE"  # charge one tick against the bid's escrow
    RELEASE = "RELEASE"  # refund whatever is left of the escrow and close it


class State:
    PENDING = "PENDING"
    DONE = "DONE"
    SKIPPED = "SKIPPED"  # wallet said the hold is closed/unknown (e.g. user cancelled): nothing to do
    DEAD = "DEAD"        # exhausted retries; needs an operator (and blocks that bid's later items)


class Item(Base):
    """A durable unit of money work. `key` is unique, so ingesting the same event twice is a no-op,
    and it doubles as the wallet idempotency key, so executing an item twice is a no-op too."""

    __tablename__ = "settlement_items"

    id: Mapped[int] = mapped_column(PK_TYPE, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String(160), unique=True)
    kind: Mapped[str] = mapped_column(String(8))
    bid_id: Mapped[str] = mapped_column(String(64))
    user_id: Mapped[str] = mapped_column(String(64))
    snapshot_id: Mapped[str | None] = mapped_column(String(40))
    amount_micro: Mapped[int] = mapped_column(BigInteger, default=0)  # charge amount (CAPTURE)
    refunded_micro: Mapped[int | None] = mapped_column(BigInteger)    # filled in for RELEASE
    state: Mapped[str] = mapped_column(String(8), default=State.PENDING)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    last_error: Mapped[str | None] = mapped_column(String(200))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    done_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        Index("ix_items_due", "state", "next_attempt_at", "id"),
        Index("ix_items_bid", "bid_id", "id"),
    )
