from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from .models import Item, Kind, State
from .wallet_client import Permanent, SettlementWallet, Transient

MAX_ATTEMPTS = 8


@dataclass
class Stats:
    done: int = 0
    skipped: int = 0
    retried: int = 0
    dead: int = 0
    deferred: int = 0  # releases held back because an earlier item for the bid is unfinished


def backoff(attempts: int) -> timedelta:
    return timedelta(seconds=min(2 ** attempts, 30))


def process_pending(sf: sessionmaker, wallet: SettlementWallet, now: datetime | None = None,
                    limit: int = 200, max_attempts: int = MAX_ATTEMPTS) -> Stats:
    """Execute due items in id order. Safe to run from many workers at once (row locks + SKIP LOCKED)
    and safe to run twice (wallet calls are idempotent on `key`).

    Ordering rule: a RELEASE never runs while an earlier item of the same bid is PENDING or DEAD,
    otherwise it would refund money that a still-pending capture is about to charge."""
    now = now or datetime.now(timezone.utc)
    st = Stats()
    with sf() as db:
        rows = db.scalars(
            select(Item).where(Item.state == State.PENDING, Item.next_attempt_at <= now)
            .order_by(Item.id).limit(limit).with_for_update(skip_locked=True)
        ).all()
        blocked: set[str] = set()
        for it in rows:
            if it.bid_id in blocked or (it.kind == Kind.RELEASE and _earlier_unfinished(db, it)):
                st.deferred += 1
                continue
            try:
                if it.kind == Kind.CAPTURE:
                    wallet.capture(it.bid_id, it.amount_micro, it.key)
                else:
                    it.refunded_micro = wallet.release(it.bid_id, it.key)
                it.state, it.done_at, it.last_error = State.DONE, now, None
                st.done += 1
            except Permanent as e:
                it.state, it.done_at, it.last_error = State.SKIPPED, now, str(e)[:200]
                st.skipped += 1
            except Transient as e:
                it.attempts += 1
                it.last_error = str(e)[:200]
                blocked.add(it.bid_id)
                if it.attempts >= max_attempts:
                    it.state = State.DEAD
                    st.dead += 1
                else:
                    it.next_attempt_at = now + backoff(it.attempts)
                    st.retried += 1
            db.flush()  # so later "earlier unfinished?" checks in this pass see the new state
        db.commit()
    return st


def _earlier_unfinished(db: Session, it: Item) -> bool:
    n = db.scalar(select(func.count()).select_from(Item).where(
        Item.bid_id == it.bid_id, Item.id < it.id, Item.state.in_((State.PENDING, State.DEAD))))
    return bool(n)


def reconcile(db: Session, wallet: SettlementWallet) -> list[str]:
    """Compare what we believe we captured per bid with what the wallet says. Empty list == clean."""
    problems = []
    bids = db.scalars(select(Item.bid_id).where(Item.kind == Kind.CAPTURE, Item.state == State.DONE).distinct())
    for bid_id in bids:
        mine = db.scalar(select(func.sum(Item.amount_micro)).where(
            Item.bid_id == bid_id, Item.kind == Kind.CAPTURE, Item.state == State.DONE)) or 0
        hold = wallet.get_hold(bid_id)
        if hold is None:
            problems.append(f"{bid_id}: wallet has no hold but we captured {mine}")
        elif hold["captured_micro"] != mine:
            problems.append(f"{bid_id}: settlement captured {mine} != wallet captured {hold['captured_micro']}")
    return problems
