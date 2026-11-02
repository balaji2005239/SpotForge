"""Uniform-price clearing for one GPU market. Pure and deterministic.

Same snapshot in -> byte-identical result out (see `digest`). No clocks, no randomness,
no I/O: the whole thing can be replayed from a stored snapshot.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable

from .models import BidIn, ClearingResult, Fill, PricingRule


def _validate(bids: list[BidIn], capacity: int, reserve_price: int) -> None:
    if capacity < 0:
        raise ValueError("capacity must be >= 0")
    if reserve_price < 0:
        raise ValueError("reserve_price must be >= 0")
    seen: set[str] = set()
    for b in bids:
        if b.qty <= 0:
            raise ValueError(f"bid {b.bid_id}: qty must be > 0")
        if b.max_price < 0:
            raise ValueError(f"bid {b.bid_id}: max_price must be >= 0")
        if b.bid_id in seen:
            raise ValueError(f"duplicate bid_id {b.bid_id}")
        seen.add(b.bid_id)


def clear(
    snapshot_id: str,
    gpu_type: str,
    bids: Iterable[BidIn],
    capacity: int,
    reserve_price: int,
    rule: PricingRule = PricingRule.HIGHEST_REJECTED,
) -> ClearingResult:
    bids = list(bids)
    _validate(bids, capacity, reserve_price)

    # Priority: price desc, then earlier arrival (so incumbents win ties), then id for total order.
    ordered = sorted(bids, key=lambda b: (-b.max_price, b.seq, b.bid_id))

    remaining = capacity
    fills: list[Fill] = []
    fill_qty: dict[str, int] = {}
    unmet_prices: list[int] = []  # prices of bids that lost (fully or partly) at the margin
    rejected: list[str] = []

    for b in ordered:
        if b.max_price < reserve_price:
            rejected.append(b.bid_id)
            continue
        if remaining == 0:
            rejected.append(b.bid_id)
            unmet_prices.append(b.max_price)
        elif b.qty <= remaining:
            fills.append(Fill(b.bid_id, b.qty))
            fill_qty[b.bid_id] = b.qty
            remaining -= b.qty
        elif b.divisible:
            fills.append(Fill(b.bid_id, remaining))
            fill_qty[b.bid_id] = remaining
            remaining = 0
            unmet_prices.append(b.max_price)  # part of its demand went unserved
        else:
            # Doesn't fit and can't be split: skip it, lower bids may still fit (max utilisation).
            rejected.append(b.bid_id)
            unmet_prices.append(b.max_price)

    price_of = {b.bid_id: b.max_price for b in bids}
    lowest_accepted = min((price_of[f.bid_id] for f in fills), default=None)

    if lowest_accepted is None:
        clearing_price = reserve_price
    elif rule is PricingRule.LOWEST_ACCEPTED:
        clearing_price = max(reserve_price, lowest_accepted)
    else:
        # Only losers priced <= the lowest winner may set the price; a skipped indivisible bid
        # above a winner must not push the price over that winner's own bid.
        eligible = [p for p in unmet_prices if p <= lowest_accepted]
        clearing_price = max([reserve_price, *eligible])

    preempted = tuple(
        b.bid_id for b in ordered if b.running and fill_qty.get(b.bid_id, 0) < b.qty
    )
    filled_qty = capacity - remaining

    canonical = {
        "snapshot_id": snapshot_id,
        "gpu_type": gpu_type,
        "rule": rule.value,
        "capacity": capacity,
        "reserve": reserve_price,
        "price": clearing_price,
        "fills": [[f.bid_id, f.qty] for f in fills],
        "rejected": rejected,
        "preempted": list(preempted),
    }
    digest = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()

    return ClearingResult(
        snapshot_id=snapshot_id,
        gpu_type=gpu_type,
        rule=rule,
        clearing_price=clearing_price,
        capacity=capacity,
        filled_qty=filled_qty,
        fills=tuple(fills),
        rejected=tuple(rejected),
        preempted=preempted,
        digest=digest,
    )
