"""Background loops: tick every N seconds, consume bid events."""
from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable

from spotcommon.config import GPU_TYPES
from sqlalchemy.orm import sessionmaker

from .events import apply_event
from .lease import TICKER, Fence, holds_lease, try_acquire
from .publish import tick_message
from .tick import run_tick

log = logging.getLogger("market")
CapacityFn = Callable[[], Awaitable[dict[str, dict[str, int]]]]


def _tick_market(session_factory: sessionmaker, gpu: str, gpus: int, tick_s: int, grace_s: int,
                 fence: Fence | None, now, publish=None) -> tuple[str, str | None]:
    """One market, one transaction. Returns ("fenced"|"skipped"|"ok", snapshot_id)."""
    with session_factory() as db:
        if fence and not holds_lease(db, TICKER, fence, now):  # fenced off: a newer leader exists
            db.rollback()
            return "fenced", None
        t = run_tick(db, gpu, gpus, tick_s, grace_s=grace_s, epoch=fence.epoch if fence else None)
        msg = tick_message(t) if t else None
        db.commit()
        if msg and publish:  # only after the commit, and only by the (fenced) leader
            publish(msg)
        return ("ok", t.snapshot_id) if t else ("skipped", None)


async def tick_once(session_factory: sessionmaker, get_capacity: CapacityFn, tick_s: int,
                    grace_s: int = 0, fence: Fence | None = None, now=None, publish=None) -> list[str]:
    """Clear every market once. If capacity is unknown we SKIP the tick: clearing against a
    bogus capacity of 0 would preempt every running job."""
    try:
        caps = await get_capacity()
    except Exception as e:
        log.warning("capacity unavailable (%s); skipping tick", e)
        return []
    done = []
    for gpu in GPU_TYPES:
        state, snap = await asyncio.to_thread(
            _tick_market, session_factory, gpu, caps.get(gpu, {}).get("gpus", 0), tick_s, grace_s, fence, now, publish)
        if state == "fenced":
            log.warning("lost the ticker lease (epoch %s); stopping", fence.epoch if fence else None)
            break
        if snap:
            done.append(snap)
    return done


async def ticker_step(session_factory: sessionmaker, get_capacity: CapacityFn, tick_s: int, grace_s: int,
                      holder: str, lease_s: float, now=None, publish=None) -> list[str] | None:
    """One iteration of a replica: try to lead, and tick only if we do. None = standing by."""
    def acquire() -> int | None:
        with session_factory() as db:
            epoch = try_acquire(db, TICKER, holder, lease_s, now)
            db.commit()
            return epoch

    epoch = await asyncio.to_thread(acquire)
    if epoch is None:
        return None
    return await tick_once(session_factory, get_capacity, tick_s, grace_s, Fence(holder, epoch), now, publish)


async def tick_loop(session_factory: sessionmaker, get_capacity: CapacityFn, tick_s: int,
                    stop: asyncio.Event, grace_s: int = 0, holder: str | None = None,
                    lease_s: float = 15, publish=None) -> None:
    """holder=None runs without election (single process: tests, dev stack)."""
    while not stop.is_set():
        started = time.monotonic()
        try:
            if holder is None:
                await tick_once(session_factory, get_capacity, tick_s, grace_s, publish=publish)
            else:
                await ticker_step(session_factory, get_capacity, tick_s, grace_s, holder, lease_s,
                                  publish=publish)
        except Exception:
            log.exception("tick failed")
        await asyncio.sleep(max(0.0, tick_s - (time.monotonic() - started)))


def http_capacity_fn(base_url: str) -> CapacityFn:
    import httpx

    async def get() -> dict:
        async with httpx.AsyncClient(base_url=base_url, timeout=2.0) as c:
            r = await c.get("/v1/capacity")
            r.raise_for_status()
            return r.json()

    return get


def _apply(session_factory: sessionmaker, topic: str, payload: dict) -> None:
    with session_factory() as db:
        apply_event(db, topic, payload)
        db.commit()


async def consume_loop(session_factory: sessionmaker, bootstrap: str) -> None:
    from spotcommon.events import consume

    topics = ["bid.submitted", "bid.cancelled", "bid.repriced"]
    async for topic, payload, commit in consume(bootstrap, "market", topics):
        await asyncio.to_thread(_apply, session_factory, topic, payload)
        await commit()  # offset only after the DB commit -> at-least-once, handler is idempotent
