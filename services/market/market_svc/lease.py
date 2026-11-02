"""Leader lease with a fencing token, stored in the market database.

Only the lease holder may run the ticker. Every acquisition by a *different* holder bumps `epoch`; a tick is
written only if the writer still owns (holder, epoch) when it commits. The check takes the lease row's lock in the
same transaction as the tick, so a deposed leader that wakes up late (GC pause, network stall) cannot write, and a
new leader cannot take over in the middle of a tick. The unique (gpu_type, tick_no) constraint remains as a backstop."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from spotcommon.db import Base
from sqlalchemy import BigInteger, DateTime, String, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapped, Session, mapped_column

TICKER = "ticker"


class Lease(Base):
    __tablename__ = "leases"

    name: Mapped[str] = mapped_column(String(40), primary_key=True)
    holder: Mapped[str] = mapped_column(String(80))
    epoch: Mapped[int] = mapped_column(BigInteger, default=1)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


@dataclass(frozen=True)
class Fence:
    holder: str
    epoch: int


def _utc(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)  # sqlite drops tzinfo


def try_acquire(db: Session, name: str, holder: str, ttl_s: float, now: datetime | None = None) -> int | None:
    """Acquire or renew. Returns the fencing epoch if `holder` leads after this call, else None.
    The caller commits."""
    now = now or datetime.now(timezone.utc)
    row = db.scalar(select(Lease).where(Lease.name == name).with_for_update())
    if row is None:
        try:
            with db.begin_nested():
                db.add(Lease(name=name, holder=holder, epoch=1, expires_at=now + timedelta(seconds=ttl_s)))
                db.flush()
        except IntegrityError:  # someone else created it first
            return None
        return 1
    if row.holder == holder:  # renewal keeps the epoch: still the same reign
        row.expires_at = now + timedelta(seconds=ttl_s)
        return row.epoch
    if _utc(row.expires_at) <= now:  # previous leader missed its renewals: take over under a new epoch
        row.holder, row.epoch = holder, row.epoch + 1
        row.expires_at = now + timedelta(seconds=ttl_s)
        return row.epoch
    return None


def holds_lease(db: Session, name: str, fence: Fence, now: datetime | None = None) -> bool:
    """The fencing check. Locks the lease row until the caller's transaction ends."""
    now = now or datetime.now(timezone.utc)
    row = db.scalar(select(Lease).where(Lease.name == name).with_for_update())
    return bool(row and row.holder == fence.holder and row.epoch == fence.epoch and _utc(row.expires_at) > now)
