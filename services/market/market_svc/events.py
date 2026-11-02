"""Idempotent projection of bid.* events into the market's order book."""
from __future__ import annotations

from sqlalchemy.orm import Session

from .models import BookBid, Status


def _from_payload(p: dict, status: str) -> BookBid:
    return BookBid(
        bid_id=p["bid_id"], seq=p["seq"], user_id=p["user_id"], gpu_type=p["gpu_type"], qty=p["qty"],
        max_price_micro=p["max_price_micro"], duration_s=p["duration_s"], divisible=p.get("divisible", False),
        hold_amount_micro=p.get("hold_amount_micro", 0), status=status, price_version=p.get("price_version", 0),
    )


def apply_event(db: Session, topic: str, p: dict) -> str:
    """Returns what happened (for logs/tests). Safe to call any number of times with the same event."""
    existing = db.get(BookBid, p["bid_id"])
    if topic == "bid.submitted":
        if existing:  # duplicate delivery, or a cancel that overtook the submit (tombstone): keep it
            return "ignored"
        db.add(_from_payload(p, Status.QUEUED))
        return "queued"
    if topic == "bid.repriced":
        if existing is None:  # overtook the submit (different topics, no ordering): the payload is the full bid
            db.add(_from_payload(p, Status.QUEUED))
            return "queued"
        if existing.status not in Status.LIVE or p.get("price_version", 0) <= existing.price_version:
            return "ignored"  # cancelled/finished, duplicate, or an older raise arriving late
        existing.max_price_micro = p["max_price_micro"]
        existing.hold_amount_micro = p.get("hold_amount_micro", existing.hold_amount_micro)
        existing.price_version = p["price_version"]
        if existing.grace_until_tick is not None:  # on notice: the raise puts it back in the auction
            existing.grace_until_tick = existing.grace_deadline_at = None
            return "repriced_cleared_notice"
        return "repriced"
    if topic == "bid.cancelled":
        if existing is None:  # cancel before submit: leave a tombstone so the late submit is ignored
            db.add(_from_payload(p, Status.CANCELLED))
            return "tombstoned"
        if existing.status in Status.LIVE:
            was_running = existing.status == Status.RUNNING
            existing.status = Status.CANCELLED
            existing.alloc_qty = 0
            return "cancelled_running" if was_running else "cancelled"
        return "ignored"
    return "unknown_topic"
