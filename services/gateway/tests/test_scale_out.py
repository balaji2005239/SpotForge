"""Gateway replicas sharing state through Redis (fakeredis): throttles, bid limits and the live feed."""
import asyncio
import json
from contextlib import ExitStack

import fakeredis
import fakeredis.aioredis
import httpx
import pytest
from bid_svc.main import create_app as create_bid
from bid_svc.wallet_client import WalletClient
from fastapi.testclient import TestClient
from gateway_svc.hub import CHANNEL, Hub
from gateway_svc.main import create_app as create_gateway
from gateway_svc.ratelimit import RateLimiter
from market_svc.main import create_app as create_market
from wallet_svc.main import create_app as create_wallet

SECRET = "test-secret-that-is-at-least-32-bytes-long"
PW = "correct horse"


def tick(gpu="A100", no=1, price=800_000, **kw):
    return {"gpu_type": gpu, "tick_no": no, "clearing_price_micro": price, "capacity": 8, "filled_qty": 2,
            "at": "2026-01-01T00:00:00+00:00", "preempted": [], "completed": [], "warned": [], "tick_seconds": 5, **kw}


# ---------- rate limiter ----------
class Clock:
    t = 0.0

    def __call__(self):
        return self.t


@pytest.mark.parametrize("use_redis", [False, True])
def test_limiter_counts_windows_and_resets(use_redis):
    srv = fakeredis.FakeServer()
    rl = RateLimiter(fakeredis.FakeRedis(server=srv, decode_responses=True) if use_redis else None, clock=Clock())
    assert [rl.hit("k", 60) for _ in range(3)] == [1, 2, 3]
    assert rl.count("k", 60) == 3 and rl.count("other", 60) == 0
    rl.reset("k")
    assert rl.count("k", 60) == 0
    assert [rl.allow("b", 2, 10) for _ in range(4)] == [True, True, False, False]


def test_memory_window_expires():
    clock = Clock()
    rl = RateLimiter(None, clock=clock)
    rl.hit("k", 10)
    rl.hit("k", 10)
    clock.t = 11
    assert rl.count("k", 10) == 0 and rl.hit("k", 10) == 1


def test_redis_window_expires_by_ttl():
    srv = fakeredis.FakeServer()
    r = fakeredis.FakeRedis(server=srv, decode_responses=True)
    RateLimiter(r).hit("k", 30)
    assert 0 < r.ttl("rl:k") <= 30


def test_limiter_falls_back_to_local_counting_when_redis_dies():
    class Dead:
        def __getattr__(self, name):
            def boom(*a, **k):
                raise ConnectionError("redis down")
            return boom

    rl = RateLimiter(Dead())
    assert rl.hit("k", 60) == 1 and rl.hit("k", 60) == 2 and rl.count("k", 60) == 2
    rl.reset("k")  # must not raise either


# ---------- hub ----------
def run(coro):
    return asyncio.run(coro)


def test_hub_dedupes_and_fans_out_to_all_clients():
    async def go():
        hub = Hub(lambda: None, poll_s=60)
        a, b = hub.subscribe(), hub.subscribe()
        assert hub.ingest(tick(no=1)) is True
        assert hub.ingest(tick(no=1)) is False  # same tick from the second source
        assert hub.ingest(tick(no=0)) is False  # stale
        assert hub.ingest(tick(no=2, price=900_000)) is True
        got = [(await a.get())["tick_no"], (await a.get())["tick_no"]]
        assert got == [1, 2] and b.qsize() == 2 and a.empty()
        snap = await hub.snapshot()
        assert snap["markets"]["A100"]["clearing_price_micro"] == 900_000 and snap["tick_seconds"] == 5
    run(go())


def test_slow_client_drops_oldest_without_blocking_others():
    async def go():
        hub = Hub(lambda: None, queue_size=3)
        slow, fast = hub.subscribe(), hub.subscribe()
        for n in range(1, 8):
            hub.ingest(tick(no=n))
            if n % 2:
                await fast.get() if fast.qsize() else None
        assert [m["tick_no"] for m in [slow.get_nowait() for _ in range(slow.qsize())]] == [5, 6, 7]
    run(go())


def test_redis_message_reaches_clients_and_poll_covers_a_missed_one():
    async def go():
        srv = fakeredis.FakeServer()
        pub = fakeredis.FakeRedis(server=srv, decode_responses=True)
        state = {"markets": {"A100": tick(no=1)}, "tick_seconds": 5}

        async def fetch():
            return state

        hub = Hub(fetch, poll_s=60, redis_async=fakeredis.aioredis.FakeRedis(server=srv, decode_responses=True),
                  safety_poll_s=0.2)
        hub.start()
        await hub.snapshot()
        q = hub.subscribe()
        await asyncio.sleep(0.1)  # let the subscription establish
        pub.publish(CHANNEL, json.dumps(tick(no=2)))
        assert (await asyncio.wait_for(q.get(), 2))["tick_no"] == 2  # fast path
        state["markets"]["A100"] = tick(no=3)  # a publish that never arrived: the safety poll finds it
        assert (await asyncio.wait_for(q.get(), 2))["tick_no"] == 3
        await hub.stop()
    run(go())


# ---------- gateway replicas ----------
@pytest.fixture()
def replicas():
    """Two gateways (own databases and hubs) in front of one set of services, sharing one Redis."""
    srv = fakeredis.FakeServer()
    with ExitStack() as stack:
        wallet = create_wallet("sqlite://")
        wallet_http = stack.enter_context(TestClient(wallet))
        apps = {"wallet": wallet, "bid": create_bid("sqlite://", wallet=WalletClient(wallet_http)),
                "market": create_market("sqlite://")}
        for a in apps.values():
            if a is not wallet:
                stack.enter_context(TestClient(a))

        def gateway():
            ups = {n: httpx.AsyncClient(transport=httpx.ASGITransport(app=a), base_url="http://svc")
                   for n, a in apps.items()}
            gw = create_gateway("sqlite://", upstreams=ups, jwt_secret=SECRET, ws_poll_s=0.05,
                                redis_sync=fakeredis.FakeRedis(server=srv, decode_responses=True),
                                redis_async=fakeredis.aioredis.FakeRedis(server=srv, decode_responses=True),
                                bid_rate=(3, 60))
            return stack.enter_context(TestClient(gw))

        yield gateway(), gateway(), srv


def test_login_throttle_is_shared_between_replicas(replicas):
    a, b, _ = replicas
    a.post("/auth/signup", json={"username": "alice", "password": PW})
    bad = {"username": "alice", "password": "wrong-password"}
    assert [a.post("/auth/login", json=bad).status_code for _ in range(3)] == [401] * 3
    assert [b.post("/auth/login", json=bad).status_code for _ in range(2)] == [401] * 2  # the 5th failure
    assert a.post("/auth/login", json=bad).status_code == 429
    assert b.post("/auth/login", json={"username": "alice", "password": PW}).status_code == 429


def test_bid_rate_limit_is_per_user_and_shared(replicas):
    a, b, _ = replicas
    hs = {}
    for name in ("alice", "bob"):
        r = a.post("/auth/signup", json={"username": name, "password": PW})
        hs[name] = {"Authorization": f"Bearer {r.json()['access_token']}"}
        a.post(f"/api/wallet/v1/wallets/{name}/deposit", headers=hs[name],
               json={"amount_micro": 500_000_000, "idempotency_key": f"f-{name}"})
    body = {"gpu_type": "A100", "qty": 1, "max_price_micro": 2_000_000, "duration_s": 600}
    codes = [(a if i % 2 else b).post("/api/bid/v1/bids", json=body, headers=hs["alice"]).status_code
             for i in range(5)]  # alternate replicas: the limit is global
    assert codes == [201, 201, 201, 429, 429]
    assert a.post("/api/bid/v1/bids", json=body, headers=hs["bob"]).status_code == 201  # others unaffected


def test_websocket_on_any_replica_gets_redis_ticks(replicas):
    a, b, srv = replicas
    with b.websocket_connect("/api/market/ws/markets") as ws:
        assert ws.receive_json()["type"] == "snapshot"
        fakeredis.FakeRedis(server=srv, decode_responses=True).publish(CHANNEL, json.dumps(tick(no=99, price=1_234_567)))
        msg = ws.receive_json()
    assert msg["type"] == "tick" and msg["tick_no"] == 99 and msg["clearing_price_micro"] == 1_234_567
