from __future__ import annotations

from datetime import datetime, timedelta, timezone

from spotcommon.outbox import emit
from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import Node


def _aware(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)  # sqlite returns naive datetimes


def register(db: Session, node_id: str, gpu_type: str, gpus: int, now: datetime) -> Node:
    """Idempotent: re-registering an existing node refreshes it and brings it back UP."""
    n = db.get(Node, node_id)
    if n is None:
        n = Node(id=node_id, gpu_type=gpu_type, gpus=gpus, status="UP", last_heartbeat=now, registered_at=now)
        db.add(n)
    else:
        n.gpu_type, n.gpus, n.status, n.last_heartbeat, n.down_until = gpu_type, gpus, "UP", now, None
    emit(db, "node.registered", gpu_type, {"node_id": node_id, "gpu_type": gpu_type, "gpus": gpus})
    db.flush()
    return n


def heartbeat(db: Session, node_id: str, now: datetime) -> bool | None:
    """Returns None for unknown node, True if this heartbeat revived a FAILED node, else False."""
    n = db.get(Node, node_id)
    if n is None:
        return None
    if n.down_until is not None:
        if now < _aware(n.down_until):
            return False  # simulated crash window: heartbeats are lost
        n.down_until = None
    revived = n.status == "FAILED"
    n.status, n.last_heartbeat = "UP", now
    if revived:
        emit(db, "node.registered", n.gpu_type, {"node_id": n.id, "gpu_type": n.gpu_type, "gpus": n.gpus})
    return revived


def mark_failed(db: Session, node_id: str, reason: str, down_s: float = 0,
                now: datetime | None = None) -> bool:
    n = db.get(Node, node_id)
    if n is None or n.status == "FAILED":
        return False
    n.status = "FAILED"
    if down_s > 0:
        n.down_until = (now or datetime.now(timezone.utc)) + timedelta(seconds=down_s)
    emit(db, "node.failed", n.gpu_type,
         {"node_id": n.id, "gpu_type": n.gpu_type, "gpus": n.gpus, "reason": reason})
    return True


def sweep(db: Session, now: datetime, timeout_s: float) -> list[str]:
    """Fail every UP node whose heartbeat is older than timeout_s. Returns the failed ids."""
    cutoff = now - timedelta(seconds=timeout_s)
    failed = []
    for n in db.scalars(select(Node).where(Node.status == "UP")):
        if _aware(n.last_heartbeat) < cutoff and mark_failed(db, n.id, "heartbeat_timeout"):
            failed.append(n.id)
    return failed


def capacity(db: Session, gpu_types: tuple[str, ...]) -> dict[str, dict[str, int]]:
    out = {g: {"gpus": 0, "nodes": 0, "failed_nodes": 0} for g in gpu_types}
    for n in db.scalars(select(Node)):
        row = out.setdefault(n.gpu_type, {"gpus": 0, "nodes": 0, "failed_nodes": 0})
        if n.status == "UP":
            row["gpus"] += n.gpus
            row["nodes"] += 1
        else:
            row["failed_nodes"] += 1
    return out
