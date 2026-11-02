"""Fixed-window counters. Redis-backed so every gateway replica shares one count; in-memory otherwise.
If Redis is unreachable we fall back to the local counter (fail open, per replica) rather than lock everyone out."""
from __future__ import annotations

import logging
import time

log = logging.getLogger("gateway")


class RateLimiter:
    def __init__(self, redis=None, clock=time.monotonic) -> None:
        self.redis = redis  # sync redis client or None
        self._clock = clock
        self._mem: dict[str, tuple[int, float]] = {}

    # --- local counters ---
    def _mem_hit(self, key: str, window_s: int) -> int:
        n, start = self._mem.get(key, (0, self._clock()))
        if self._clock() - start >= window_s:
            n, start = 0, self._clock()
        self._mem[key] = (n + 1, start)
        return n + 1

    def _mem_count(self, key: str, window_s: int) -> int:
        n, start = self._mem.get(key, (0, 0.0))
        return n if self._clock() - start < window_s else 0

    # --- public API ---
    def hit(self, key: str, window_s: int) -> int:
        """Record one event; returns how many happened in the current window."""
        if self.redis is not None:
            try:
                k = f"rl:{key}"
                n = int(self.redis.incr(k))
                if n == 1:
                    self.redis.expire(k, window_s)
                return n
            except Exception as e:
                log.warning("rate limiter: redis unavailable (%s); using local counters", e)
        return self._mem_hit(key, window_s)

    def count(self, key: str, window_s: int) -> int:
        if self.redis is not None:
            try:
                return int(self.redis.get(f"rl:{key}") or 0)
            except Exception as e:
                log.warning("rate limiter: redis unavailable (%s); using local counters", e)
        return self._mem_count(key, window_s)

    def reset(self, key: str) -> None:
        self._mem.pop(key, None)
        if self.redis is not None:
            try:
                self.redis.delete(f"rl:{key}")
            except Exception:
                pass

    def allow(self, key: str, limit: int, window_s: int) -> bool:
        return self.hit(key, window_s) <= limit
