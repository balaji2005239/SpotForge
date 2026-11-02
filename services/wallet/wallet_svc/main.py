from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException
from spotcommon.db import Base, make_engine, make_session_factory
from spotcommon.migrate import upgrade_to_head
from sqlalchemy import select
from sqlalchemy.orm import Session

from . import ledger
from .models import Hold
from .schemas import CaptureIn, DepositIn, HoldIn, HoldOut, IncreaseIn, ReleaseIn, TxnOut, WalletOut


def create_app(database_url: str | None = None, auto_create: bool | None = None) -> FastAPI:
    url = database_url or os.getenv("WALLET_DATABASE_URL", "sqlite://")
    engine = make_engine(url)
    Session_ = make_session_factory(engine)
    if auto_create is None:
        auto_create = os.getenv("AUTO_CREATE_TABLES", "0") == "1" or url.startswith("sqlite")

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if os.getenv("MIGRATE_ON_START") == "1":  # production: versioned schema (see migrations/)
            upgrade_to_head(Path(__file__).resolve().parents[1], url)
        elif auto_create:  # dev/test
            Base.metadata.create_all(engine)
        yield

    app = FastAPI(title="Wallet / Ledger Service", version="0.1.0", lifespan=lifespan)
    app.state.session_factory = Session_
    app.state.engine = engine

    def get_db() -> Iterator[Session]:
        with Session_() as db:
            try:
                yield db
                db.commit()
            except Exception:
                db.rollback()
                raise

    def _map(exc: ledger.LedgerError) -> HTTPException:
        if isinstance(exc, ledger.InsufficientFunds):
            return HTTPException(402, str(exc))
        if isinstance(exc, ledger.NotFound):
            return HTTPException(404, str(exc))
        if isinstance(exc, ledger.Conflict):
            return HTTPException(409, str(exc))
        return HTTPException(400, str(exc))

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.post("/v1/wallets/{user_id}/deposit", response_model=TxnOut)
    def deposit(user_id: str, body: DepositIn, db: Session = Depends(get_db)):
        try:
            r = ledger.deposit(db, user_id, body.amount_micro, f"deposit:{body.idempotency_key}")
        except ledger.LedgerError as e:
            raise _map(e)
        return TxnOut(txn_id=r.txn_id, replayed=r.replayed)

    @app.get("/v1/wallets/{user_id}", response_model=WalletOut)
    def wallet(user_id: str, db: Session = Depends(get_db)):
        a, h = ledger.balances(db, user_id)
        return WalletOut(user_id=user_id, available_micro=a, held_micro=h)

    @app.post("/v1/holds", response_model=TxnOut, status_code=201)
    def create_hold(body: HoldIn, db: Session = Depends(get_db)):
        try:
            r = ledger.place_hold(db, body.user_id, body.hold_id, body.amount_micro)
        except ledger.LedgerError as e:
            raise _map(e)
        return TxnOut(txn_id=r.txn_id, replayed=r.replayed)

    def _hold_out(h: Hold) -> HoldOut:
        return HoldOut(hold_id=h.id, user_id=h.user_id, amount_micro=h.amount,
                       captured_micro=h.captured, released_micro=h.released,
                       remaining_micro=h.remaining, status=h.status)

    @app.get("/v1/wallets/{user_id}/holds", response_model=list[HoldOut])
    def user_holds(user_id: str, limit: int = 50, db: Session = Depends(get_db)):
        rows = db.scalars(select(Hold).where(Hold.user_id == user_id)
                          .order_by(Hold.created_at.desc()).limit(min(limit, 200))).all()
        return [_hold_out(h) for h in rows]

    @app.get("/v1/holds/{hold_id}", response_model=HoldOut)
    def get_hold(hold_id: str, db: Session = Depends(get_db)):
        h = db.get(Hold, hold_id)
        if not h:
            raise HTTPException(404, "hold not found")
        return _hold_out(h)

    @app.post("/v1/holds/{hold_id}/capture", response_model=TxnOut)
    def capture(hold_id: str, body: CaptureIn, db: Session = Depends(get_db)):
        try:
            r = ledger.capture(db, hold_id, body.amount_micro, f"capture:{body.idempotency_key}")
        except ledger.LedgerError as e:
            raise _map(e)
        return TxnOut(txn_id=r.txn_id, replayed=r.replayed)

    @app.post("/v1/holds/{hold_id}/increase", response_model=TxnOut)
    def increase(hold_id: str, body: IncreaseIn, db: Session = Depends(get_db)):
        try:
            r = ledger.increase_hold(db, hold_id, body.additional_micro, f"increase:{body.idempotency_key}")
        except ledger.LedgerError as e:
            raise _map(e)
        return TxnOut(txn_id=r.txn_id, replayed=r.replayed)

    @app.post("/v1/holds/{hold_id}/release", response_model=TxnOut)
    def release(hold_id: str, body: ReleaseIn, db: Session = Depends(get_db)):
        try:
            r, refunded = ledger.release(db, hold_id, f"release:{body.idempotency_key}")
        except ledger.LedgerError as e:
            raise _map(e)
        return TxnOut(txn_id=r.txn_id, replayed=r.replayed, refunded_micro=refunded)

    @app.get("/v1/admin/integrity")
    def integrity(db: Session = Depends(get_db)):
        problems = ledger.verify_integrity(db)
        return {"ok": not problems, "problems": problems}

    return app


app = create_app() if os.getenv("WALLET_DATABASE_URL") else None
