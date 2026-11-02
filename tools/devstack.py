"""Run the whole platform in ONE process: no Docker, Kafka, Postgres or Redis.

    python tools/devstack.py [--port 8000] [--no-bots] [--static frontend/dist]

Same services, same code, same HTTP contracts as docker-compose; only the plumbing differs:
sqlite instead of Postgres, an in-process bus instead of Kafka, an asyncio loop instead of Celery.
The API gateway is the only public door (/auth/*, /api/<service>/*, same paths as docker-compose); sign up as
"admin" to get the fleet and checks tab.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import random
import sys
import tempfile
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for p in ("libs/spotcommon", "libs/clearing_core", "services/wallet", "services/bid", "services/capacity",
          "services/market", "services/settlement", "services/gateway"):
    sys.path.insert(0, str(ROOT / p))

os.environ.setdefault("TICK_SECONDS", "2")  # faster than prod's 5s so the demo feels alive
os.environ.setdefault("WS_POLL_S", "0.3")

import httpx  # noqa: E402
from bid_svc.main import create_app as create_bid  # noqa: E402
from bid_svc.projection import apply_allocation_event  # noqa: E402
from bid_svc.wallet_client import WalletClient  # noqa: E402
from capacity_svc import service as cap_service  # noqa: E402
from capacity_svc.main import create_app as create_capacity  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.responses import RedirectResponse  # noqa: E402
from fastapi.staticfiles import StaticFiles  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from gateway_svc.main import create_app as create_gateway  # noqa: E402
from market_svc.events import apply_event  # noqa: E402
from market_svc.main import create_app as create_market  # noqa: E402
from market_svc.worker import tick_loop  # noqa: E402
from settlement_svc.ingest import ingest_cleared  # noqa: E402
from settlement_svc.main import create_app as create_settlement  # noqa: E402
from settlement_svc.processor import process_pending  # noqa: E402
from settlement_svc.wallet_client import SettlementWallet  # noqa: E402
from spotcommon.config import GPU_TYPES, RESERVE_MICRO, CommonSettings  # noqa: E402
from spotcommon.outbox import run_relay  # noqa: E402
from wallet_svc.main import create_app as create_wallet  # noqa: E402

NODES = [("T4", 2, 8), ("A100", 2, 8), ("H100", 1, 8)]


def build(bots: bool = True, static_dir: Path | None = None, db_dir: Path | None = None) -> FastAPI:
    tick_s = CommonSettings().tick_seconds
    grace_s = int(os.environ.get("GRACE_SECONDS", "10"))  # short notice so it is watchable on a 2s tick
    db_dir = db_dir or Path(tempfile.mkdtemp(prefix="spotdev-"))  # fresh state every run
    url = lambda name: f"sqlite:///{db_dir / (name + '.db')}"  # noqa: E731  (file-backed: safe under threads)
    wallet = create_wallet(url("wallet"))
    wallet_http = TestClient(wallet)
    bid = create_bid(url("bid"), wallet=WalletClient(wallet_http))
    capacity = create_capacity(url("capacity"), heartbeat_timeout_s=6)
    market = create_market(url("market"))
    settlement = create_settlement(url("settlement"), wallet=SettlementWallet(wallet_http))
    subs = {"wallet": wallet, "bid": bid, "capacity": capacity, "market": market, "settlement": settlement}
    sf = {name: a.state.session_factory for name, a in subs.items()}
    # The gateway is the only public door. It reaches the services in-process (the compose stack does it over HTTP).
    gateway = create_gateway(
        url("gateway"), jwt_secret=os.environ.get("JWT_SECRET", "devstack-secret-devstack-secret-1234"),
        upstreams={n: httpx.AsyncClient(transport=httpx.ASGITransport(app=a), base_url="http://internal")
                   for n, a in subs.items()},
        ws_poll_s=float(os.environ["WS_POLL_S"]))

    inbox: dict[str, asyncio.Queue] = {}

    class LocalBus:
        """Like Kafka: publishing only enqueues. A separate consumer applies events afterwards, so a relay
        never holds one service's database lock while writing to another's (that deadlocks sqlite)."""

        async def publish(self, topic: str, key: str, payload: dict) -> None:
            inbox["q"].put_nowait((topic, payload))

    def handle(topic: str, payload: dict) -> None:
        if topic in ("bid.submitted", "bid.cancelled", "bid.repriced"):
            with sf["market"]() as db:
                apply_event(db, topic, payload)
                db.commit()
        elif topic.startswith("allocation."):
            with sf["bid"]() as db:
                apply_allocation_event(db, topic, payload)
                db.commit()
        elif topic == "market.cleared":
            with sf["settlement"]() as db:
                ingest_cleared(db, payload)
                db.commit()

    async def dispatch(stop: asyncio.Event):
        while not stop.is_set():
            topic, payload = await inbox["q"].get()
            try:
                await asyncio.to_thread(handle, topic, payload)  # single consumer => per-topic order kept
            except Exception as e:  # dev stack: log and keep going
                print(f"[devstack] dropped {topic}: {e}", file=sys.stderr)

    bus = LocalBus()

    def read_capacity():
        with sf["capacity"]() as db:
            return cap_service.capacity(db, GPU_TYPES)

    async def get_capacity():
        return await asyncio.to_thread(read_capacity)

    async def heartbeats(stop: asyncio.Event):
        ids = []
        def register() -> None:
            with sf["capacity"]() as db:
                for gpu, count, gpus in NODES:
                    for i in range(count):
                        nid = f"{gpu.lower()}-{i + 1}"
                        cap_service.register(db, nid, gpu, gpus, datetime.now(timezone.utc))
                        ids.append(nid)
                db.commit()

        def beat() -> None:
            with sf["capacity"]() as db:
                for nid in ids:
                    cap_service.heartbeat(db, nid, datetime.now(timezone.utc))
                db.commit()

        await asyncio.to_thread(register)
        while not stop.is_set():
            await asyncio.to_thread(beat)
            await asyncio.sleep(2)

    async def sweeper(stop: asyncio.Event):
        while not stop.is_set():
            def sweep() -> None:
                with sf["capacity"]() as db:
                    cap_service.sweep(db, datetime.now(timezone.utc), 6)
                    db.commit()

            await asyncio.to_thread(sweep)
            await asyncio.sleep(2)

    async def settle_loop(stop: asyncio.Event):
        w = SettlementWallet(wallet_http)
        while not stop.is_set():
            await asyncio.to_thread(process_pending, sf["settlement"], w)
            await asyncio.sleep(0.5)

    async def demand_bots(stop: asyncio.Event):
        rng = random.Random(7)
        users = [f"bot-{i}" for i in range(1, 6)]
        bid_http = TestClient(bid)
        for u in users:
            # idempotency keys are global in the wallet, so each bot needs its own
            wallet_http.post(f"/v1/wallets/{u}/deposit",
                             json={"amount_micro": 500_000_000, "idempotency_key": f"seed-{u}"})
        await asyncio.sleep(3)
        while not stop.is_set():
            gpu = rng.choices(GPU_TYPES, weights=[2, 3, 2])[0]
            price = int(RESERVE_MICRO[gpu] * rng.uniform(1.0, 3.2))
            await asyncio.to_thread(
                bid_http.post, "/v1/bids",
                json={"user_id": rng.choice(users), "gpu_type": gpu, "qty": rng.choice([1, 2, 4, 4, 8]),
                      "max_price_micro": price, "duration_s": rng.choice([60, 90, 120, 180]),
                      "divisible": rng.random() < 0.3})
            await asyncio.sleep(rng.uniform(1.5, 4))

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        stop = asyncio.Event()
        inbox["q"] = asyncio.Queue()
        async with AsyncExitStack() as stack:
            for sub in (*subs.values(), gateway):  # sub-apps don't get their own lifespan; run them here
                await stack.enter_async_context(sub.router.lifespan_context(sub))
            tasks = [
                asyncio.create_task(dispatch(stop)),
                asyncio.create_task(heartbeats(stop)), asyncio.create_task(sweeper(stop)),
                asyncio.create_task(tick_loop(sf["market"], get_capacity, tick_s, stop, grace_s,
                                         holder="devstack", lease_s=10)),
                asyncio.create_task(settle_loop(stop)),
                *(asyncio.create_task(run_relay(sf[n], bus, n, interval=0.1, stop=stop))
                  for n in ("bid", "capacity", "market")),
            ]
            if bots:
                tasks.append(asyncio.create_task(demand_bots(stop)))
            yield
            stop.set()
            for t in tasks:
                t.cancel()

    app = FastAPI(title="GPU Spot Market (dev stack)", lifespan=lifespan)
    app.include_router(gateway.router)  # /auth/*, /api/<service>/* (authorized), /api/market/ws/markets
    if static_dir and static_dir.exists():
        app.mount("/", StaticFiles(directory=static_dir, html=True), name="ui")
    else:
        @app.get("/")
        def index():
            return RedirectResponse("/docs")
    return app


def main() -> None:
    import uvicorn

    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--no-bots", action="store_true")
    ap.add_argument("--static", default=str(ROOT / "frontend" / "dist"))
    a = ap.parse_args()
    uvicorn.run(build(not a.no_bots, Path(a.static)), host=a.host, port=a.port, log_level="warning")


if __name__ == "__main__":
    main()
