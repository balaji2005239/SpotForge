from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI
from spotcommon.config import CommonSettings
from spotcommon.db import Base, make_engine, make_session_factory
from spotcommon.migrate import upgrade_to_head
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .ingest import ingest_cleared
from .models import Item, Kind, State
from .processor import reconcile
from .wallet_client import SettlementWallet


async def consume_loop(session_factory, bootstrap: str) -> None:
    from spotcommon.events import consume

    async for _topic, payload, commit in consume(bootstrap, "settlement", ["market.cleared"]):
        def ingest(payload=payload) -> None:
            with session_factory() as db:
                ingest_cleared(db, payload)
                db.commit()

        await asyncio.to_thread(ingest)
        await commit()  # offset only after the items are durable


def create_app(database_url: str | None = None, wallet: SettlementWallet | None = None,
               auto_create: bool | None = None) -> FastAPI:
    url = database_url or os.getenv("SETTLEMENT_DATABASE_URL", "sqlite://")
    engine = make_engine(url)
    Session_ = make_session_factory(engine)
    wallet_client = wallet or SettlementWallet.from_url(os.getenv("WALLET_URL", "http://localhost:8001"))
    if auto_create is None:
        auto_create = os.getenv("AUTO_CREATE_TABLES", "0") == "1" or url.startswith("sqlite")

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if os.getenv("MIGRATE_ON_START") == "1":  # production: versioned schema (see migrations/)
            upgrade_to_head(Path(__file__).resolve().parents[1], url)
        elif auto_create:  # dev/test
            Base.metadata.create_all(engine)
        task = None
        if os.getenv("RUN_CONSUMER") == "1":
            task = asyncio.create_task(consume_loop(Session_, CommonSettings().kafka_bootstrap))
        yield
        if task:
            task.cancel()

    app = FastAPI(title="Settlement Service", version="0.1.0", lifespan=lifespan)
    app.state.session_factory = Session_

    def get_db() -> Iterator[Session]:
        with Session_() as db:
            yield db

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.get("/v1/settlement/stats")
    def stats(db: Session = Depends(get_db)):
        rows = db.execute(select(Item.kind, Item.state, func.count(), func.coalesce(func.sum(Item.amount_micro), 0),
                                 func.coalesce(func.sum(Item.refunded_micro), 0)).group_by(Item.kind, Item.state)).all()
        out = {f"{k}.{s}": {"count": n, "amount_micro": a, "refunded_micro": r} for k, s, n, a, r in rows}
        captured = db.scalar(select(func.coalesce(func.sum(Item.amount_micro), 0))
                             .where(Item.kind == Kind.CAPTURE, Item.state == State.DONE))
        return {"by_state": out, "total_captured_micro": captured}

    @app.get("/v1/settlement/dead")
    def dead(db: Session = Depends(get_db)):
        return [{"key": i.key, "bid_id": i.bid_id, "kind": i.kind, "attempts": i.attempts, "error": i.last_error}
                for i in db.scalars(select(Item).where(Item.state == State.DEAD).limit(200))]

    @app.post("/v1/admin/reconcile")
    def run_reconcile(db: Session = Depends(get_db)):
        problems = reconcile(db, wallet_client)
        return {"ok": not problems, "problems": problems}

    return app


app = create_app() if os.getenv("SETTLEMENT_DATABASE_URL") else None
