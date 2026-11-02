"""Live market feed for WebSocket clients, shared by every connection in this gateway process.

One source per process instead of one per client:
  * Redis pub/sub (fast path, published by the market's leader after each committed tick), and
  * an HTTP poll of the market's /v1/markets/latest (always on): the whole feed when Redis is not configured,
    a slow safety net that fills any gap when it is.
Messages are de-duplicated by tick number, so both paths can run together. A client that can't keep up loses
its oldest queued messages instead of stalling everyone else."""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Awaitable, Callable

log = logging.getLogger("gateway")
CHANNEL = "market.ticks"
Fetch = Callable[[], Awaitable[dict]]


class Hub:
    def __init__(self, fetch_latest: Fetch, poll_s: float = 0.5, redis_async=None, safety_poll_s: float = 5.0,
                 queue_size: int = 50) -> None:
        self.fetch_latest, self.redis = fetch_latest, redis_async
        self.poll_s = safety_poll_s if redis_async is not None else poll_s
        self.queue_size = queue_size
        self.latest: dict[str, dict] = {}
        self.tick_seconds: int | None = None
        self._seen: dict[str, int] = {}
        self._subs: set[asyncio.Queue] = set()
        self._tasks: list[asyncio.Task] = []

    # ----- clients -----
    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(self.queue_size)
        self._subs.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subs.discard(q)

    @property
    def clients(self) -> int:
        return len(self._subs)

    async def snapshot(self) -> dict:
        if not self.latest:
            await self.refresh(announce=False)
        return {"type": "snapshot", "tick_seconds": self.tick_seconds, "markets": dict(self.latest)}

    # ----- sources -----
    def ingest(self, m: dict, announce: bool = True) -> bool:
        gpu, no = m["gpu_type"], m["tick_no"]
        if no <= self._seen.get(gpu, -1):
            return False  # duplicate or stale (Redis and the poller both delivered it)
        self._seen[gpu] = no
        self.latest[gpu] = m
        if m.get("tick_seconds"):
            self.tick_seconds = m["tick_seconds"]
        if announce:
            msg = {"type": "tick", **m}
            for q in list(self._subs):
                if q.full():
                    with contextlib.suppress(asyncio.QueueEmpty):
                        q.get_nowait()  # slow client: drop its oldest message
                q.put_nowait(msg)
        return True

    async def refresh(self, announce: bool = True) -> None:
        data = await self.fetch_latest()
        self.tick_seconds = data.get("tick_seconds") or self.tick_seconds
        for m in data["markets"].values():
            self.ingest(m, announce)

    async def _poll(self) -> None:
        while True:
            try:
                await self.refresh()
            except Exception as e:
                log.warning("market poll failed: %s", e)
            await asyncio.sleep(self.poll_s)

    async def _listen(self) -> None:
        while True:
            try:
                pubsub = self.redis.pubsub()
                await pubsub.subscribe(CHANNEL)
                async for item in pubsub.listen():
                    if item.get("type") == "message":
                        self.ingest(json.loads(item["data"]))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("redis feed dropped (%s); retrying (polling covers the gap)", e)
                await asyncio.sleep(1.0)

    def start(self) -> None:
        self._tasks = [asyncio.create_task(self._poll())]
        if self.redis is not None:
            self._tasks.append(asyncio.create_task(self._listen()))

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
