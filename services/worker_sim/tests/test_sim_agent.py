import asyncio

import httpx
from worker_sim.agent import parse_nodes, run_node


def test_parse_nodes():
    assert parse_nodes("A100:2x8,T4:1x4") == [("A100", "a100-1", 8), ("A100", "a100-2", 8), ("T4", "t4-1", 4)]
    assert parse_nodes("") == []


def test_node_registers_then_heartbeats_and_re_registers_after_404():
    calls = []

    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(req.url.path)
        if req.url.path.endswith("/heartbeat") and calls.count(req.url.path) == 1:
            return httpx.Response(404)  # capacity service lost us (e.g. DB reset)
        return httpx.Response(201 if "register" in req.url.path else 200, json={})

    async def go():
        import random
        stop = asyncio.Event()
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://x") as c:
            t = asyncio.create_task(run_node(c, "A100", "a100-1", 8, 0.01, 0.0, 0.0, random.Random(1), stop))
            await asyncio.sleep(0.1)
            stop.set()
            await t

    asyncio.run(go())
    assert calls[0] == "/v1/nodes/register"
    assert calls.count("/v1/nodes/register") >= 2 and "/v1/nodes/a100-1/heartbeat" in calls
