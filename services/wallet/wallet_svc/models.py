from __future__ import annotations

from datetime import datetime, timezone

from spotcommon.db import Base
from sqlalchemy import BigInteger, CheckConstraint, DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

# BIGINT in Postgres; plain INTEGER in sqlite so autoincrement works in tests.
PK_TYPE = BigInteger().with_variant(Integer, "sqlite")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Account(Base):
    """One row per ledger account. `balance` is a cache of SUM(entries.amount), verified by
    ledger.verify_integrity(). User accounts can never go negative (DB-enforced)."""

    __tablename__ = "accounts"

    id: Mapped[str] = mapped_column(String(80), primary_key=True)  # e.g. user:42:available
    owner_id: Mapped[str | None] = mapped_column(String(64), index=True)
    kind: Mapped[str] = mapped_column(String(16))  # AVAILABLE | HELD | REVENUE | EXTERNAL
    balance: Mapped[int] = mapped_column(BigInteger, default=0)

    __table_args__ = (CheckConstraint("kind = 'EXTERNAL' OR balance >= 0", name="ck_balance_nonneg"),)


class Txn(Base):
    __tablename__ = "txns"

    id: Mapped[int] = mapped_column(PK_TYPE, primary_key=True, autoincrement=True)
    idem_key: Mapped[str] = mapped_column(String(160), unique=True)
    fingerprint: Mapped[str] = mapped_column(String(64))  # hash of the legs; detects key reuse
    kind: Mapped[str] = mapped_column(String(24))
    ref: Mapped[str | None] = mapped_column(String(80), index=True)  # hold_id / allocation ref
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class Entry(Base):
    """Double-entry leg. Entries of one txn always sum to zero."""

    __tablename__ = "entries"

    id: Mapped[int] = mapped_column(PK_TYPE, primary_key=True, autoincrement=True)
    txn_id: Mapped[int] = mapped_column(ForeignKey("txns.id"), index=True)
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"))
    amount: Mapped[int] = mapped_column(BigInteger)

    __table_args__ = (Index("ix_entries_account", "account_id"),)


class Hold(Base):
    """Escrow for one bid. amount = captured + released + remaining."""

    __tablename__ = "holds"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)  # == bid_id
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    amount: Mapped[int] = mapped_column(BigInteger)
    captured: Mapped[int] = mapped_column(BigInteger, default=0)
    released: Mapped[int] = mapped_column(BigInteger, default=0)
    status: Mapped[str] = mapped_column(String(12), default="ACTIVE")  # ACTIVE | CLOSED
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_hold_amount_pos"),
        CheckConstraint("captured + released <= amount", name="ck_hold_conserved"),
    )

    @property
    def remaining(self) -> int:
        return self.amount - self.captured - self.released
