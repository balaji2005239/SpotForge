from __future__ import annotations

from datetime import datetime, timezone

from spotcommon.db import Base
from sqlalchemy import CheckConstraint, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Node(Base):
    __tablename__ = "nodes"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    gpu_type: Mapped[str] = mapped_column(String(16), index=True)
    gpus: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(8), default="UP")  # UP | FAILED
    last_heartbeat: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    registered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    down_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))  # chaos: ignore heartbeats until then

    __table_args__ = (CheckConstraint("gpus > 0", name="ck_node_gpus"),)
