from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import Iterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query
from spotcommon.config import GPU_TYPES, RESERVE_MICRO, CommonSettings
from spotcommon.db import Base, make_engine, make_session_factory
from spotcommon.migrate import upgrade_to_head
from spotcommon.money import hold_amount
from spotcommon.outbox import emit, run_relay
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .models import Bid, BidStatus
from .schemas import BidIn, BidOut, BidReprice
from .wallet_client import HoldClosed, InsufficientFunds, WalletClient, WalletUnavailable


def _out(b: Bid) -> BidOut:
    return BidOut(bid_id=b.bid_id, user_id=b.user_id, gpu_type=b.gpu_type, qty=b.qty,
                  max_price_micro=b.max_price_micro, duration_s=b.duration_s, divisible=b.divisible,
                  checkpointable=b.checkpointable, priority=b.priority,
                  hold_amount_micro=b.hold_amount_micro, status=b.status,
                  reject_reason=b.reject_reason, seq=b.seq)


async def _sweep_loop(sweep, every_s: float = 10.0) -> None:
    while True:
        await asyncio.sleep(every_s)
        try:
            await asyncio.to_thread(sweep)
        except Exception:  # never let a bad sweep kill the loop
            pass


def create_app(database_url: str | None = None, wallet: WalletClient | None = None,
               auto_create: bool | None = None) -> FastAPI:
    url = database_url or os.getenv("BID_DATABASE_URL", "sqlite://")
    engine = make_engine(url)
    Session_ = make_session_factory(engine)
    wallet_client = wallet or WalletClient.from_url(os.getenv("WALLET_URL", "http://localhost:8001"))
    if auto_create is None:
        auto_create = os.getenv("AUTO_CREATE_TABLES", "0") == "1" or url.startswith("sqlite")

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if os.getenv("MIGRATE_ON_START") == "1":  # production: versioned schema (see migrations/)
            upgrade_to_head(Path(__file__).resolve().parents[1], url)
        elif auto_create:  # dev/test
            Base.metadata.create_all(engine)
        relay, consumer, sweeper = None, None, None
        if os.getenv("RUN_CONSUMER") == "1":
            from .projection import consume_allocations

            consumer = asyncio.create_task(consume_allocations(Session_, CommonSettings().kafka_bootstrap))
        if os.getenv("RUN_RELAY") == "1":
            from spotcommon.events import KafkaBus

            bus = KafkaBus(CommonSettings().kafka_bootstrap)
            relay = asyncio.create_task(run_relay(Session_, bus, "bid"))
            sweeper = asyncio.create_task(_sweep_loop(_.state.sweep_pending))
        yield
        for t in (relay, consumer, sweeper):
            if t:
                t.cancel()

    tick_s = CommonSettings().tick_seconds
    app = FastAPI(title="Bid Service", version="0.1.0", lifespan=lifespan)
    app.state.session_factory = Session_

    def get_db() -> Iterator[Session]:
        with Session_() as db:
            yield db

    def _emit(db: Session, topic: str, b: Bid, **extra) -> None:
        emit(db, topic, b.gpu_type, {
            "bid_id": b.bid_id, "user_id": b.user_id, "gpu_type": b.gpu_type, "qty": b.qty,
            "max_price_micro": b.max_price_micro, "duration_s": b.duration_s,
            "divisible": b.divisible, "checkpointable": b.checkpointable,
            "priority": b.priority, "seq": b.seq, **extra})


    def _activate(db: Session, bid: Bid) -> Bid:
        """Steps 2+3 of a submit: escrow via the wallet (idempotent on bid_id, so safe to repeat), then flip
        PENDING -> ACTIVE/REJECTED and emit the event in ONE transaction. The conditional UPDATE makes a
        client retry and the sweeper racing each other harmless: exactly one of them emits the event."""
        try:
            wallet_client.hold(bid.user_id, bid.bid_id, bid.hold_amount_micro)
        except InsufficientFunds as e:
            db.execute(update(Bid).where(Bid.bid_id == bid.bid_id, Bid.status == BidStatus.PENDING)
                       .values(status=BidStatus.REJECTED, reject_reason=str(e)[:120]))
            db.commit()
            db.expire(bid)
            return bid
        except WalletUnavailable:  # outcome unknown: stays PENDING; a retry or the sweeper finishes it
            raise HTTPException(503, f"wallet unavailable; bid {bid.bid_id} is PENDING, retry the same request")
        won = db.execute(update(Bid).where(Bid.bid_id == bid.bid_id, Bid.status == BidStatus.PENDING)
                         .values(status=BidStatus.ACTIVE)).rowcount == 1
        db.expire(bid)
        if won:
            _emit(db, "bid.submitted", bid, hold_amount_micro=bid.hold_amount_micro)
        db.commit()
        db.expire(bid)
        return bid

    def sweep_pending(min_age_s: float = 10.0, now: datetime | None = None) -> int:
        """Finish bids stuck in PENDING (the wallet was down after the PENDING row was written)."""
        cutoff = (now or datetime.now(timezone.utc)) - timedelta(seconds=min_age_s)
        with Session_() as db:
            ids = db.scalars(select(Bid.bid_id).where(Bid.status == BidStatus.PENDING,
                                                      Bid.created_at <= cutoff)).all()
        moved = 0
        for bid_id in ids:
            with Session_() as db:
                b = db.scalar(select(Bid).where(Bid.bid_id == bid_id))
                if b is not None and b.status == BidStatus.PENDING:
                    try:
                        moved += _activate(db, b).status != BidStatus.PENDING
                    except HTTPException:
                        pass  # wallet still down; next sweep
        return moved

    app.state.sweep_pending = sweep_pending

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.post("/v1/bids", response_model=BidOut, status_code=201)
    def submit_bid(body: BidIn, db: Session = Depends(get_db)):
        try:
            body.validate_gpu()
        except ValueError as e:
            raise HTTPException(422, str(e))
        if body.max_price_micro < RESERVE_MICRO[body.gpu_type]:
            raise HTTPException(422, f"max_price below reserve {RESERVE_MICRO[body.gpu_type]} for {body.gpu_type}")

        # Client retry with the same request id returns the original bid (idempotent submit). If that bid is
        # still PENDING (the wallet call timed out last time) the retry finishes the job instead of
        # handing back a bid that nothing would ever activate.
        if body.client_request_id:
            prior = db.scalar(select(Bid).where(Bid.user_id == body.user_id,
                                                Bid.client_request_id == body.client_request_id))
            if prior:
                return _out(_activate(db, prior) if prior.status == BidStatus.PENDING else prior)

        amount = hold_amount(body.max_price_micro, body.qty, body.duration_s, tick_s)
        bid = Bid(bid_id=f"bid_{uuid.uuid4().hex[:20]}", user_id=body.user_id, gpu_type=body.gpu_type,
                  qty=body.qty, max_price_micro=body.max_price_micro, duration_s=body.duration_s,
                  divisible=body.divisible, checkpointable=body.checkpointable, priority=body.priority,
                  hold_amount_micro=amount, status=BidStatus.PENDING,
                  client_request_id=body.client_request_id)
        db.add(bid)
        try:
            db.commit()  # step 1: durable PENDING record (no DB txn held across the HTTP call)
        except IntegrityError:
            db.rollback()
            prior = db.scalar(select(Bid).where(Bid.user_id == body.user_id,
                                                Bid.client_request_id == body.client_request_id))
            return _out(prior)
        return _out(_activate(db, bid))

    @app.get("/v1/bids/{bid_id}", response_model=BidOut)
    def get_bid(bid_id: str, db: Session = Depends(get_db)):
        b = db.scalar(select(Bid).where(Bid.bid_id == bid_id))
        if not b:
            raise HTTPException(404, "bid not found")
        return _out(b)

    @app.get("/v1/bids", response_model=list[BidOut])
    def list_bids(user_id: str | None = None, gpu_type: str | None = None,
                  status: str | None = None, limit: int = Query(50, le=200),
                  db: Session = Depends(get_db)):
        q = select(Bid).order_by(Bid.seq.desc()).limit(limit)
        if user_id:
            q = q.where(Bid.user_id == user_id)
        if gpu_type:
            q = q.where(Bid.gpu_type == gpu_type)
        if status:
            q = q.where(Bid.status == status)
        return [_out(b) for b in db.scalars(q)]

    @app.patch("/v1/bids/{bid_id}", response_model=BidOut)
    def raise_bid(bid_id: str, body: BidReprice, db: Session = Depends(get_db)):
        """Raise the max price of a live bid (queued, or running and possibly on preemption notice).
        Escrow grows first (idempotent), then the bid and a bid.repriced event commit together."""
        b = db.scalar(select(Bid).where(Bid.bid_id == bid_id))
        if not b:
            raise HTTPException(404, "bid not found")
        if b.status not in (BidStatus.ACTIVE, BidStatus.RUNNING):
            raise HTTPException(409, f"cannot change a {b.status} bid")
        if body.max_price_micro == b.max_price_micro:
            return _out(b)  # idempotent: same request twice
        if body.max_price_micro < b.max_price_micro:
            raise HTTPException(422, "You can only raise a bid, not lower it.")
        old_version, new_price = b.price_version, body.max_price_micro
        new_hold = hold_amount(new_price, b.qty, b.duration_s, tick_s)
        extra = new_hold - b.hold_amount_micro
        if extra > 0:
            try:  # key names the exact change, so a retry replays instead of escrowing twice
                wallet_client.increase_hold(b.bid_id, extra, f"raise:{b.bid_id}:{old_version + 1}:{new_price}")
            except InsufficientFunds as e:
                raise HTTPException(402, str(e))
            except HoldClosed:
                raise HTTPException(409, "That job just ended.")
            except WalletUnavailable:
                raise HTTPException(503, "wallet unavailable; retry")
        # Optimistic update: a concurrent raise/cancel changed the row, so don't clobber it.
        n = db.execute(update(Bid).where(Bid.bid_id == bid_id, Bid.price_version == old_version,
                                        Bid.status.in_((BidStatus.ACTIVE, BidStatus.RUNNING)))
                       .values(max_price_micro=new_price, hold_amount_micro=max(new_hold, b.hold_amount_micro),
                               price_version=old_version + 1)).rowcount
        if n != 1:
            db.rollback()
            raise HTTPException(409, "This bid changed while you were editing it. Try again.")
        db.expire(b)
        b = db.scalar(select(Bid).where(Bid.bid_id == bid_id))
        _emit(db, "bid.repriced", b, hold_amount_micro=b.hold_amount_micro, price_version=b.price_version)
        db.commit()
        return _out(b)

    @app.delete("/v1/bids/{bid_id}", response_model=BidOut)
    def cancel_bid(bid_id: str, db: Session = Depends(get_db)):
        """Cancel = ONE local transaction (status + bid.cancelled event). It never calls the wallet.

        The refund is settlement's job: the market announces the cancelled bid in its next market.cleared event,
        AFTER every charge it ever made for the bid, and settlement releases the escrow behind them. Releasing
        here instead raced with charges still in flight (the job ran, the hold closed, the capture was skipped)
        and, when the wallet answer got lost, left a cancelled-looking job running for free."""
        b = db.scalar(select(Bid).where(Bid.bid_id == bid_id))
        if not b:
            raise HTTPException(404, "bid not found")
        if b.status == BidStatus.CANCELLED:
            return _out(b)  # idempotent
        if b.status not in (BidStatus.ACTIVE, BidStatus.RUNNING):
            raise HTTPException(409, f"cannot cancel a {b.status} bid")
        won = db.execute(update(Bid).where(Bid.bid_id == bid_id, Bid.status.in_((BidStatus.ACTIVE, BidStatus.RUNNING)))
                         .values(status=BidStatus.CANCELLED)).rowcount == 1
        if not won:  # status changed under us (e.g. the market just finished it)
            db.rollback()
            raise HTTPException(409, "bid changed while cancelling; retry")
        db.expire(b)
        _emit(db, "bid.cancelled", b)
        db.commit()
        db.refresh(b)
        return _out(b)

    @app.get("/v1/markets")
    def markets():
        return [{"gpu_type": g, "reserve_price_micro": RESERVE_MICRO[g]} for g in GPU_TYPES]

    return app


app = create_app() if os.getenv("BID_DATABASE_URL") else None
