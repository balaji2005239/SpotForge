"""Simulated GPU nodes. Each node registers with the capacity service and heartbeats.

NODES="A100:3x8,H100:2x8,T4:4x8"  -> 3 A100 nodes with 8 GPUs, 2 H100 nodes ...
FAIL_RATE: per-heartbeat probability that a node "crashes" (stops heartbeating) for DOWN_S seconds,
then re-registers -> exercises node.failed, capacity shrink and preemption end to end.
"""
from __future__ import annotations

import asyncio
import os
import random

import httpx


def parse_nodes(spec: str) -> list[tuple[str, str, int]]:
    """'A100:2x8,T4:1x4' -> [('A100','a100-1',8), ('A100','a100-2',8), ('T4','t4-1',4)]"""
    out = []
    for part in filter(None, (p.strip() for p in spec.split(","))):
        gpu, rest = part.split(":")
        count, gpus = rest.lower().split("x")
        out += [(gpu.upper(), f"{gpu.lower()}-{i + 1}", int(gpus)) for i in range(int(count))]
    return out


async def run_node(client: httpx.AsyncClient, gpu: str, node_id: str, gpus: int, hb_s: float,
                   fail_rate: float, down_s: float, rng: random.Random, stop: asyncio.Event) -> None:
    async def register():
        while not stop.is_set():
            try:
                body = {"node_id": node_id, "gpu_type": gpu, "gpus": gpus}
                (await client.post("/v1/nodes/register", json=body)).raise_for_status()
                return
            except httpx.HTTPError:
                await asyncio.sleep(1)

    await register()
    while not stop.is_set():
        await asyncio.sleep(hb_s)
        if rng.random() < fail_rate:  # crash: go silent, the sweeper will mark us FAILED
            await asyncio.sleep(down_s)
            await register()
            continue
        try:
            r = await client.post(f"/v1/nodes/{node_id}/heartbeat")
            if r.status_code == 404:
                await register()
        except httpx.HTTPError:
            pass


async def main() -> None:
    base = os.getenv("CAPACITY_URL", "http://localhost:8003")
    nodes = parse_nodes(os.getenv("NODES", "T4:2x8,A100:2x8,H100:1x8"))
    hb_s = float(os.getenv("HEARTBEAT_S", "3"))
    fail_rate = float(os.getenv("FAIL_RATE", "0"))
    down_s = float(os.getenv("DOWN_S", "30"))
    rng, stop = random.Random(int(os.getenv("SEED", "42"))), asyncio.Event()
    async with httpx.AsyncClient(base_url=base, timeout=3.0) as client:
        await asyncio.gather(*(run_node(client, g, n, c, hb_s, fail_rate, down_s, rng, stop) for g, n, c in nodes))


if __name__ == "__main__":
    asyncio.run(main())
