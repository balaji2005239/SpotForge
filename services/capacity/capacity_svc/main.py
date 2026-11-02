from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field
from spotcommon.config import GPU_TYPES, CommonSettings
from spotcommon.db import Base, make_engine, make_session_factory
from spotcommon.migrate import upgrade_to_head
from spotcommon.outbox import run_relay
from sqlalchemy.orm import Session

from . import service


class RegisterIn(BaseModel):
    node_id: str = Field(min_length=1, max_length=64)
    gpu_type: str
    gpus: int = Field(ge=1, le=64)


def create_app(database_url: str | None = None, heartbeat_timeout_s: float | None = None,
               auto_create: bool | None = None) -> FastAPI:
    url = database_url or os.getenv("CAPACITY_DATABASE_URL", "sqlite://")
    timeout_s = heartbeat_timeout_s or float(os.getenv("HEARTBEAT_TIMEOUT_S", "15"))
    engine = make_engine(url)
    Session_ = make_session_factory(engine)
    if auto_create is None:
        auto_create = os.getenv("AUTO_CREATE_TABLES", "0") == "1" or url.startswith("sqlite")

    async def sweeper(stop: asyncio.Event):
        while not stop.is_set():
            with Session_() as db:
                service.sweep(db, datetime.now(timezone.utc), timeout_s)
                db.commit()
            await asyncio.sleep(max(1.0, timeout_s / 3))

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if os.getenv("MIGRATE_ON_START") == "1":  # production: versioned schema (see migrations/)
            upgrade_to_head(Path(__file__).resolve().parents[1], url)
        elif auto_create:  # dev/test
            Base.metadata.create_all(engine)
        stop, tasks = asyncio.Event(), []
        if os.getenv("RUN_SWEEPER") == "1":
            tasks.append(asyncio.create_task(sweeper(stop)))
        if os.getenv("RUN_RELAY") == "1":
            from spotcommon.events import KafkaBus
            bus = KafkaBus(CommonSettings().kafka_bootstrap)
            tasks.append(asyncio.create_task(run_relay(Session_, bus, "capacity", stop=stop)))
        yield
        stop.set()
        for t in tasks:
            t.cancel()

    app = FastAPI(title="Capacity Service", version="0.1.0", lifespan=lifespan)
    app.state.session_factory = Session_

    def get_db() -> Iterator[Session]:
        with Session_() as db:
            try:
                yield db
                db.commit()
            except Exception:
                db.rollback()
                raise

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.post("/v1/nodes/register", status_code=201)
    def register(body: RegisterIn, db: Session = Depends(get_db)):
        if body.gpu_type not in GPU_TYPES:
            raise HTTPException(422, f"gpu_type must be one of {GPU_TYPES}")
        n = service.register(db, body.node_id, body.gpu_type, body.gpus, datetime.now(timezone.utc))
        return {"node_id": n.id, "status": n.status}

    @app.post("/v1/nodes/{node_id}/heartbeat")
    def heartbeat(node_id: str, db: Session = Depends(get_db)):
        revived = service.heartbeat(db, node_id, datetime.now(timezone.utc))
        if revived is None:
            raise HTTPException(404, "unknown node; register first")
        return {"node_id": node_id, "revived": revived}

    @app.post("/v1/nodes/{node_id}/fail")
    def fail(node_id: str, down_s: float = 20, db: Session = Depends(get_db)):
        """Chaos hook: crash a node now; it ignores heartbeats for `down_s` seconds, then rejoins."""
        if not service.mark_failed(db, node_id, "chaos", down_s=down_s):
            raise HTTPException(404, "node unknown or already failed")
        return {"node_id": node_id, "status": "FAILED"}

    @app.get("/v1/nodes")
    def list_nodes(db: Session = Depends(get_db)):
        from sqlalchemy import select

        from .models import Node

        return [{"node_id": n.id, "gpu_type": n.gpu_type, "gpus": n.gpus, "status": n.status,
                 "last_heartbeat": service._aware(n.last_heartbeat).isoformat()}
                for n in db.scalars(select(Node).order_by(Node.gpu_type, Node.id))]

    @app.get("/v1/capacity")
    def get_capacity(db: Session = Depends(get_db)):
        return service.capacity(db, GPU_TYPES)

    return app


app = create_app() if os.getenv("CAPACITY_DATABASE_URL") else None
