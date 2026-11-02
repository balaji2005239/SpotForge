"""Benchmarks. Run one section at a time:  python tools/bench.py clearing|tick|submit|soak

Everything here is in-process on one machine with sqlite files: it measures OUR code (clearing, DB access
pattern, request handling), not Postgres/Kafka/network. See docs/BENCHMARKS.md for what that does and doesn't mean.
"""
from __future__ import annotations

import asyncio
import json
import platform
import random
import statistics
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for sub in ("libs/spotcommon", "libs/clearing_core", "services/wallet", "services/bid", "services/capacity",
            "services/market", "services/settlement", "services/gateway", "tests"):
    sys.path.insert(0, str(ROOT / sub))


def pct(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))]


def env() -> dict:
    import os
    return {"python": platform.python_version(), "machine": platform.machine(), "cpus": os.cpu_count(),
            "os": platform.platform()}


# ---------------------------------------------------------------------------------------------------------
def bench_clearing() -> dict:
    from clearing_core.engine import clear
    from clearing_core.models import BidIn
    out = {}
    for n in (100, 1_000, 10_000, 50_000):
        rng = random.Random(n)
        bids = [BidIn(f"b{i}", rng.randint(1, 8), rng.randint(800_000, 8_000_000), i, divisible=rng.random() < .3,
                      running=i < n // 10) for i in range(n)]
        cap = max(8, n // 4)
        ts = []
        for _ in range(5):
            t = time.perf_counter()
            r = clear("s", "A100", bids, cap, 800_000)
            ts.append((time.perf_counter() - t) * 1000)
        out[n] = {"median_ms": round(statistics.median(ts), 2), "filled": r.filled_qty, "capacity": cap}
    return out


def bench_tick() -> dict:
    from market_svc.events import apply_event
    from market_svc.tick import run_tick
    from spotcommon.db import Base, make_engine, make_session_factory
    out = {}
    for n in (100, 1_000, 5_000):
        e = make_engine("sqlite://")
        Base.metadata.create_all(e)
        sf = make_session_factory(e)
        rng = random.Random(n)
        with sf() as db:
            for i in range(n):
                apply_event(db, "bid.submitted", {
                    "bid_id": f"b{i}", "seq": i + 1, "user_id": f"u{i % 50}", "gpu_type": "A100",
                    "qty": rng.randint(1, 8), "max_price_micro": rng.randint(800_000, 8_000_000),
                    "duration_s": 3600, "divisible": rng.random() < .3, "hold_amount_micro": 1})
            db.commit()
        ts = []
        for _ in range(6):
            with sf() as db:
                t = time.perf_counter()
                run_tick(db, "A100", 256, 5, grace_s=15)
                db.commit()
                ts.append((time.perf_counter() - t) * 1000)
        out[n] = {"first_tick_ms": round(ts[0], 1), "steady_median_ms": round(statistics.median(ts[1:]), 1)}
    return out


# ---------------------------------------------------------------------------------------------------------
async def _drive(client, make_req, total: int, concurrency: int):
    lat: list[float] = []
    codes: dict[int, int] = {}
    q: asyncio.Queue = asyncio.Queue()
    for i in range(total):
        q.put_nowait(i)

    async def worker():
        while True:
            try:
                i = q.get_nowait()
            except asyncio.QueueEmpty:
                return
            t = time.perf_counter()
            r = await make_req(client, i)
            lat.append((time.perf_counter() - t) * 1000)
            codes[r.status_code] = codes.get(r.status_code, 0) + 1

    t0 = time.perf_counter()
    await asyncio.gather(*(worker() for _ in range(concurrency)))
    wall = time.perf_counter() - t0
    return {"requests": total, "concurrency": concurrency, "req_per_s": round(total / wall, 1),
            "p50_ms": round(pct(lat, .5), 1), "p95_ms": round(pct(lat, .95), 1), "p99_ms": round(pct(lat, .99), 1),
            "status": codes}


async def _submit_bench() -> dict:
    import httpx
    from bid_svc.main import create_app as create_bid
    from bid_svc.wallet_client import WalletClient
    from fastapi.testclient import TestClient
    from wallet_svc.main import create_app as create_wallet

    d = Path(tempfile.mkdtemp(prefix="bench-"))
    users = [f"u{i}" for i in range(200)]
    out = {}
    with TestClient(create_wallet(f"sqlite:///{d / 'wallet.db'}")) as wallet:
        for u in users:
            wallet.post(f"/v1/wallets/{u}/deposit", json={"amount_micro": 10**12, "idempotency_key": f"d-{u}"})
        with TestClient(create_bid(f"sqlite:///{d / 'bid.db'}", wallet=WalletClient(wallet))) as bids:
            transport = httpx.ASGITransport(app=bids.app)
            async with httpx.AsyncClient(transport=transport, base_url="http://bid", timeout=60) as c:
                async def submit(client, i):
                    return await client.post("/v1/bids", json={
                        "user_id": users[i % len(users)], "gpu_type": "A100", "qty": 2, "max_price_micro": 1_500_000,
                        "duration_s": 600, "client_request_id": f"c{i}-{random.random()}"})
                for conc in (1, 8, 32):
                    out[f"submit_c{conc}"] = await _drive(c, submit, 400 if conc == 1 else 1200, conc)
                async def read(client, i):
                    return await client.get("/v1/bids", params={"user_id": users[i % len(users)]})
                out["list_bids_c32"] = await _drive(c, read, 600, 32)
            assert wallet.get("/v1/admin/integrity").json()["ok"]
    return out


def bench_submit() -> dict:
    return asyncio.run(_submit_bench())


# ---------------------------------------------------------------------------------------------------------
def bench_soak(seeds: int, rounds: int) -> dict:
    from harness import run_soak
    agg: dict[str, float] = {}
    t0 = time.perf_counter()
    for seed in range(100, 100 + seeds):
        out = run_soak(seed, rounds=rounds)  # raises on ANY invariant violation
        for k, v in out.items():
            if isinstance(v, int):
                agg[k] = agg.get(k, 0) + v
    agg["seeds"] = seeds
    agg["rounds_per_seed"] = rounds
    agg["violations"] = 0
    agg["wall_s"] = round(time.perf_counter() - t0, 1)
    return agg


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "clearing"
    res = {"clearing": bench_clearing, "tick": bench_tick, "submit": bench_submit,
           "soak": lambda: bench_soak(int(sys.argv[2]) if len(sys.argv) > 2 else 20,
                                      int(sys.argv[3]) if len(sys.argv) > 3 else 100)}[which]()
    print(json.dumps({"section": which, "env": env(), "result": res}, indent=2, default=str))
