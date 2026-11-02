"""Fan-out of cleared ticks to Redis pub/sub. Best effort by design: the database is the source of truth, and the
gateway reconciles from /v1/markets/latest, so a lost publish can only delay a tick on screen, never lose one."""
from __future__ import annotations

import json
import logging

from spotcommon.timeutil import iso_utc

from .models import Tick

log = logging.getLogger("market")
CHANNEL = "market.ticks"


def tick_message(t: Tick) -> dict:
    """The one wire shape for a tick: used by /v1/markets/latest, the WebSocket and Redis."""
    r = t.result or {}
    return {"gpu_type": t.gpu_type, "tick_no": t.tick_no, "clearing_price_micro": t.clearing_price_micro,
            "capacity": t.capacity, "filled_qty": t.filled_qty, "at": iso_utc(t.created_at),
            "preempted": r.get("preempted", []), "completed": r.get("completed", []),
            "warned": r.get("warned", []), "grace_seconds": r.get("grace_seconds"),
            "queued_unfilled": r.get("queued_unfilled", 0), "tick_seconds": r.get("tick_seconds")}


class TickPublisher:
    def __init__(self, client) -> None:
        self.client = client  # a sync redis.Redis (it runs in the tick worker thread)

    def __call__(self, message: dict) -> None:
        try:
            self.client.publish(CHANNEL, json.dumps(message))
        except Exception as e:  # never fail a committed tick because the fan-out is down
            log.warning("tick publish failed (%s); gateways will catch up by polling", e)
