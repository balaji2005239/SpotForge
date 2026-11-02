from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class PricingRule(str, Enum):
    # Price = highest bid that lost (fully or partly) at the margin, floored at reserve.
    # Winners never set their own price -> closer to incentive-compatible. Default.
    HIGHEST_REJECTED = "highest_rejected"
    # Price = lowest accepted bid, floored at reserve (the simple "last-in" rule).
    LOWEST_ACCEPTED = "lowest_accepted"


@dataclass(frozen=True, slots=True)
class BidIn:
    bid_id: str
    qty: int  # GPUs wanted
    max_price: int  # micro-dollars per GPU-hour
    seq: int  # arrival order; lower = earlier. Running jobs keep their original seq.
    divisible: bool = False  # may be partially filled
    running: bool = False  # currently holding GPUs (re-enters every tick)


@dataclass(frozen=True, slots=True)
class Fill:
    bid_id: str
    qty: int


@dataclass(frozen=True, slots=True)
class ClearingResult:
    snapshot_id: str
    gpu_type: str
    rule: PricingRule
    clearing_price: int
    capacity: int
    filled_qty: int
    fills: tuple[Fill, ...]  # winners, in priority order
    rejected: tuple[str, ...]  # bid_ids with zero fill
    preempted: tuple[str, ...]  # running bids that got less than they hold
    digest: str  # sha256 over the canonical result; replays must match
