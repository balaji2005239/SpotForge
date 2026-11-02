from __future__ import annotations

import asyncio
import os
import socket
import uuid
from collections.abc import Iterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.concurrency import run_in_threadpool
from spotcommon.config import GPU_TYPES, RESERVE_MICRO, CommonSettings
from spotcommon.db import Base, make_engine, make_session_factory
from spotcommon.migrate import upgrade_to_head
from spotcommon.outbox import run_relay
from spotcommon.timeutil import iso_utc
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .lease import TICKER, Lease
from .models import BookBid, Status, Tick
from .publish import TickPublisher, tick_message
from .tick import run_tick
from .worker import consume_loop, http_capacity_fn, tick_loop

HOLDER = f"{socket.gethostname()}-{uuid.uuid4().hex[:6]}"


def create_app(database_url: str | None = None, auto_create: bool | None = None) -> FastAPI:
    url = database_url or os.getenv("MARKET_DATABASE_URL", "sqlite://")
    engine = make_engine(url)
    Session_ = make_session_factory(engine)
    settings = CommonSettings()
    if auto_create is None:
        auto_create = os.getenv("AUTO_CREATE_TABLES", "0") == "1" or url.startswith("sqlite")

    publisher = None
    if os.getenv("REDIS_URL"):
        import redis

        publisher = TickPublisher(redis.Redis.from_url(os.environ["REDIS_URL"], socket_timeout=1))

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if os.getenv("MIGRATE_ON_START") == "1":  # production: versioned schema (see migrations/)
            upgrade_to_head(Path(__file__).resolve().parents[1], url)
        elif auto_create:  # dev/test
            Base.metadata.create_all(engine)
        stop, tasks = asyncio.Event(), []
        # Any number of replicas may set RUN_TICKER=1: they elect one leader (see lease.py).
        if os.getenv("RUN_TICKER") == "1":
            cap = http_capacity_fn(os.getenv("CAPACITY_URL", "http://localhost:8003"))
            tasks.append(asyncio.create_task(
                tick_loop(Session_, cap, settings.tick_seconds, stop, settings.grace_seconds,
                          holder=HOLDER, lease_s=settings.lease_seconds, publish=publisher)))
        if os.getenv("RUN_CONSUMER") == "1":
            tasks.append(asyncio.create_task(consume_loop(Session_, settings.kafka_bootstrap)))
        if os.getenv("RUN_RELAY") == "1":
            from spotcommon.events import KafkaBus
            bus = KafkaBus(settings.kafka_bootstrap)
            tasks.append(asyncio.create_task(run_relay(Session_, bus, "market", stop=stop)))
        yield
        stop.set()
        for t in tasks:
            t.cancel()

    app = FastAPI(title="Market (Clearing) Service", version="0.1.0", lifespan=lifespan)
    app.state.session_factory = Session_

    def get_db() -> Iterator[Session]:
        with Session_() as db:
            try:
                yield db
                db.commit()
            except Exception:
                db.rollback()
                raise

    def _check(gpu: str) -> None:
        if gpu not in GPU_TYPES:
            raise HTTPException(404, f"unknown market {gpu}")

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.get("/v1/markets/{gpu}/price")
    def price(gpu: str, db: Session = Depends(get_db)):
        _check(gpu)
        t = db.scalar(select(Tick).where(Tick.gpu_type == gpu).order_by(Tick.tick_no.desc()).limit(1))
        if not t:
            return {"gpu_type": gpu, "clearing_price_micro": RESERVE_MICRO[gpu], "tick_no": 0,
                    "capacity": 0, "filled_qty": 0}
        return {"gpu_type": gpu, "clearing_price_micro": t.clearing_price_micro, "tick_no": t.tick_no,
                "capacity": t.capacity, "filled_qty": t.filled_qty, "digest": t.digest}

    @app.get("/v1/markets/{gpu}/ticks")
    def ticks(gpu: str, limit: int = Query(100, le=1000), db: Session = Depends(get_db)):
        _check(gpu)
        rows = db.scalars(select(Tick).where(Tick.gpu_type == gpu).order_by(Tick.tick_no.desc()).limit(limit)).all()
        return [{"tick_no": t.tick_no, "clearing_price_micro": t.clearing_price_micro, "capacity": t.capacity,
                 "filled_qty": t.filled_qty, "at": iso_utc(t.created_at)} for t in reversed(rows)]

    @app.get("/v1/markets/{gpu}/book")
    def book(gpu: str, db: Session = Depends(get_db)):
        """Depth by price level (for the supply/demand curve in the UI)."""
        _check(gpu)
        rows = db.execute(
            select(BookBid.max_price_micro, BookBid.status, func.sum(BookBid.qty))
            .where(BookBid.gpu_type == gpu, BookBid.status.in_(Status.LIVE))
            .group_by(BookBid.max_price_micro, BookBid.status).order_by(BookBid.max_price_micro.desc())
        ).all()
        return [{"price_micro": p, "status": s, "qty": int(q)} for p, s, q in rows]

    @app.get("/v1/admin/leader")
    def leader(db: Session = Depends(get_db)):
        row = db.get(Lease, TICKER)
        return {"holder": row.holder, "epoch": row.epoch, "expires_at": iso_utc(row.expires_at),
                "this_replica": HOLDER} if row else {"holder": None, "this_replica": HOLDER}

    @app.get("/v1/allocations")
    def allocations(user_id: str | None = None, status: str | None = None, limit: int = Query(100, le=500),
                    db: Session = Depends(get_db)):
        q = select(BookBid).order_by(BookBid.seq.desc()).limit(limit)
        if user_id:
            q = q.where(BookBid.user_id == user_id)
        if status:
            q = q.where(BookBid.status == status)
        return [{"bid_id": b.bid_id, "user_id": b.user_id, "gpu_type": b.gpu_type, "status": b.status,
                 "qty": b.qty, "alloc_qty": b.alloc_qty, "ticks_run": b.ticks_run, "duration_s": b.duration_s,
                 "max_price_micro": b.max_price_micro,
                 "grace_deadline_at": iso_utc(b.grace_deadline_at) if b.grace_deadline_at else None}
                for b in db.scalars(q)]

    def _latest_ticks() -> dict[str, dict]:
        out = {}
        with Session_() as db:
            for gpu in GPU_TYPES:
                t = db.scalar(select(Tick).where(Tick.gpu_type == gpu).order_by(Tick.tick_no.desc()).limit(1))
                if t:
                    out[gpu] = tick_message(t)
        return out

    @app.get("/v1/markets/latest")
    def latest():
        """Newest tick of every market. The gateway polls this to drive its WebSocket."""
        return {"tick_seconds": settings.tick_seconds, "markets": _latest_ticks()}

    @app.websocket("/ws/markets")
    async def stream(ws: WebSocket):
        """Pushes a snapshot on connect, then one message per new tick per market.
        (Polls the tick table; Redis pub/sub fan-out replaces this when the market runs on several nodes.)"""
        await ws.accept()
        poll_s = float(os.getenv("WS_POLL_S", "0.5"))
        seen: dict[str, int] = {}
        try:
            first = await run_in_threadpool(_latest_ticks)
            await ws.send_json({"type": "snapshot", "tick_seconds": settings.tick_seconds, "markets": first})
            seen = {g: m["tick_no"] for g, m in first.items()}
            while True:
                await asyncio.sleep(poll_s)
                for gpu, m in (await run_in_threadpool(_latest_ticks)).items():
                    if seen.get(gpu) != m["tick_no"]:
                        seen[gpu] = m["tick_no"]
                        await ws.send_json({"type": "tick", **m})
        except (WebSocketDisconnect, RuntimeError):
            return

    @app.post("/v1/admin/tick/{gpu}")
    def admin_tick(gpu: str, capacity: int = Query(ge=0), db: Session = Depends(get_db)):
        """Dev/test hook: run one tick with an explicit capacity."""
        _check(gpu)
        t = run_tick(db, gpu, capacity, settings.tick_seconds, grace_s=settings.grace_seconds)
        if t is None:
            raise HTTPException(409, "tick already produced by another writer")
        return t.result

    return app


app = create_app() if os.getenv("MARKET_DATABASE_URL") else None
