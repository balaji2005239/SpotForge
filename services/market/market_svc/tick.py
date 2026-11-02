"""One auction tick for one market: snapshot -> clear -> apply lifecycle -> emit events."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from clearing_core import BidIn, PricingRule, clear
from spotcommon.config import RESERVE_MICRO
from spotcommon.money import tick_charge
from spotcommon.outbox import emit
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .models import BookBid, Status, Tick


def run_tick(db: Session, gpu_type: str, capacity: int, tick_s: int,
             rule: PricingRule = PricingRule.HIGHEST_REJECTED, tick_no: int | None = None,
             grace_s: int = 0, epoch: int | None = None) -> Tick | None:
    """Returns the Tick, or None if another writer already produced this (market, tick_no).
    The caller owns the transaction: everything here (tick row, bid state, outbox events) commits atomically.

    grace_s > 0: a running job that is merely OUTBID is not killed. It gets an `allocation.warned` notice and keeps
    its GPUs, unbilled, for grace_s seconds (rounded up to whole ticks), then is preempted. Its GPUs stay reserved
    meanwhile, so the auction clears against the remaining capacity and never double-books. If capacity itself
    shrank (node failure) there is nothing to hold on to: warned jobs are dropped first, then losers die at once."""
    if tick_no is None:
        tick_no = (db.scalar(select(func.max(Tick.tick_no)).where(Tick.gpu_type == gpu_type)) or 0) + 1
    snapshot_id = f"{gpu_type}:{tick_no}"
    grace_ticks = -(-grace_s // tick_s) if grace_s > 0 else 0

    live = db.scalars(
        select(BookBid).where(BookBid.gpu_type == gpu_type, BookBid.status.in_(Status.LIVE)).order_by(BookBid.seq)
    ).all()

    # --- who is still on notice, whose notice ran out, and what does the hardware still hold? ---
    expired = [b for b in live if b.grace_until_tick is not None and tick_no >= b.grace_until_tick]
    warned = [b for b in live if b.grace_until_tick is not None and b not in expired]
    held = sum(b.alloc_qty for b in live if b.status == Status.RUNNING and b not in expired)
    capacity_dropped = []
    for b in sorted(warned, key=lambda x: -x.seq):  # capacity shrank: doomed jobs go first
        if held <= capacity:
            break
        held -= b.alloc_qty
        capacity_dropped.append(b)
    warned = [b for b in warned if b not in capacity_dropped]
    dead = {b.bid_id: "grace_expired" for b in expired} | {b.bid_id: "capacity_lost" for b in capacity_dropped}

    competing = [b for b in live if b.bid_id not in dead and b not in warned]
    reserved = sum(b.alloc_qty for b in warned)
    running_qty = sum(b.alloc_qty for b in competing if b.status == Status.RUNNING)
    can_warn = grace_ticks > 0 and running_qty <= capacity - reserved  # False => real capacity loss
    newly_warned: list[BookBid] = []
    while True:
        result = clear(
            snapshot_id, gpu_type,
            [BidIn(b.bid_id, b.qty, b.max_price_micro, b.seq, b.divisible, b.status == Status.RUNNING)
             for b in competing],
            max(0, capacity - reserved), RESERVE_MICRO[gpu_type], rule,
        )
        if not can_warn:
            break
        got = {f.bid_id for f in result.fills}
        losers = [b for b in competing if b.status == Status.RUNNING and b.bid_id not in got and not b.notice_used]
        if not losers:
            break
        for b in losers:  # keep their GPUs reserved, then re-clear for everyone else
            newly_warned.append(b)
            reserved += b.alloc_qty
        competing = [b for b in competing if b not in losers]

    tick = Tick(gpu_type=gpu_type, tick_no=tick_no, snapshot_id=snapshot_id, epoch=epoch, digest=result.digest,
                clearing_price_micro=result.clearing_price, capacity=capacity, filled_qty=result.filled_qty,
                result={})
    try:
        with db.begin_nested():  # claim the tick number first; a loser changes nothing else
            db.add(tick)
            db.flush()
    except IntegrityError:
        return None

    fills = {f.bid_id: f.qty for f in result.fills}
    price = result.clearing_price
    charges, preempted, completed, lifecycle = [], [], [], []

    for b in live:
        if b.bid_id in dead:
            b.status, b.alloc_qty, b.grace_until_tick, b.grace_deadline_at = Status.PREEMPTED, 0, None, None
            preempted.append(b.bid_id)
            lifecycle.append(("allocation.preempted", b,
                              {"reason": dead[b.bid_id], "clearing_price_micro": price}))

    now = datetime.now(timezone.utc)
    for b in newly_warned:
        b.notice_used = True  # one notice per bid: raising by a hair can't buy unbilled GPU time forever
        b.grace_until_tick = tick_no + grace_ticks
        b.grace_deadline_at = now + timedelta(seconds=grace_ticks * tick_s)
        lifecycle.append(("allocation.warned", b, {
            "reason": "outbid", "clearing_price_micro": price, "grace_seconds": grace_ticks * tick_s,
            "grace_until_tick": b.grace_until_tick, "deadline": b.grace_deadline_at.isoformat()}))

    for b in competing:
        q = fills.get(b.bid_id, 0)
        was_running = b.status == Status.RUNNING
        if q == 0:
            if was_running:
                b.status, b.alloc_qty = Status.PREEMPTED, 0
                preempted.append(b.bid_id)
                lifecycle.append(("allocation.preempted", b,
                                  {"reason": "outbid_or_capacity", "clearing_price_micro": price}))
            continue
        if not was_running:
            b.status = Status.RUNNING
            lifecycle.append(("allocation.created", b, {"qty": q}))
        elif b.bid_id in result.preempted:  # running divisible bid shrunk, not killed
            lifecycle.append(("allocation.resized", b, {"from_qty": b.alloc_qty, "qty": q}))
        b.alloc_qty = q
        b.ticks_run += 1
        charge = tick_charge(price, q, tick_s)
        charges.append({"bid_id": b.bid_id, "user_id": b.user_id, "qty": q, "charge_micro": charge,
                        "charge_key": f"{b.bid_id}:{snapshot_id}"})  # settlement idempotency key
        if b.ticks_run * tick_s >= b.duration_s:
            b.status = Status.COMPLETED
            completed.append(b.bid_id)
            lifecycle.append(("allocation.completed", b, {"ticks_run": b.ticks_run}))

    cancelled = []  # users cancelled these; announced AFTER all their charges, so settlement releases the escrow last
    for b in db.scalars(select(BookBid).where(BookBid.gpu_type == gpu_type, BookBid.status == Status.CANCELLED,
                                              BookBid.cancel_announced.is_(False))):
        b.cancel_announced = True
        cancelled.append(b.bid_id)

    summary = {
        "snapshot_id": snapshot_id, "tick_no": tick_no, "gpu_type": gpu_type, "tick_seconds": tick_s,
        "clearing_price_micro": price, "capacity": capacity, "filled_qty": result.filled_qty,
        "rule": result.rule.value, "digest": result.digest, "charges": charges,
        "preempted": preempted, "completed": completed, "cancelled": cancelled,
        "warned": [b.bid_id for b in newly_warned], "on_notice": len(warned) + len(newly_warned),
        "grace_seconds": grace_ticks * tick_s,
        "queued_unfilled": len(result.rejected),
    }
    tick.result = summary
    emit(db, "market.cleared", gpu_type, summary)
    for topic, b, extra in lifecycle:
        emit(db, topic, gpu_type, {"bid_id": b.bid_id, "user_id": b.user_id, "gpu_type": gpu_type,
                                   "snapshot_id": snapshot_id, **extra})
    db.flush()
    return tick
