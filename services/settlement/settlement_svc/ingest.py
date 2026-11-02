from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import Item, Kind


def ingest_cleared(db: Session, payload: dict) -> int:
    """Turn ONE market.cleared event into work items; returns how many NEW items were created.

    Everything for a tick comes from this single event on purpose: captures are created before the
    releases (lower ids), and a release can never overtake the last capture of its bid. (Reading the
    completion from a separate allocation.* topic would allow exactly that reordering.)
    """
    snap = payload["snapshot_id"]
    new = 0

    def add(key: str, **kw) -> None:
        nonlocal new
        if db.scalar(select(Item.id).where(Item.key == key)) is None:
            db.add(Item(key=key, snapshot_id=snap, **kw))
            db.flush()
            new += 1

    for c in payload.get("charges", []):
        add(c["charge_key"], kind=Kind.CAPTURE, bid_id=c["bid_id"], user_id=c["user_id"],
            amount_micro=c["charge_micro"])
    user_of = {c["bid_id"]: c["user_id"] for c in payload.get("charges", [])}
    for bid_id in [*payload.get("completed", []), *payload.get("preempted", []), *payload.get("cancelled", [])]:
        add(f"release:{bid_id}", kind=Kind.RELEASE, bid_id=bid_id, user_id=user_of.get(bid_id, ""))
    return new
