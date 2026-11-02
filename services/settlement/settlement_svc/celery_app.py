"""Celery wiring. `celery -A settlement_svc.celery_app worker -B -l info`
beat runs the processor every second; the task itself is idempotent and lock-safe, so overlapping
runs or several workers are fine."""
from __future__ import annotations

import os

from celery import Celery
from spotcommon.db import Base, make_engine, make_session_factory

from .processor import process_pending
from .wallet_client import SettlementWallet

celery = Celery("settlement", broker=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
celery.conf.update(
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    beat_schedule={"settle-pending": {"task": "settlement.process_pending", "schedule": 1.0}},
)

_state: dict = {}


def _deps():
    if not _state:
        engine = make_engine(os.getenv("SETTLEMENT_DATABASE_URL", "sqlite://"))
        if os.getenv("AUTO_CREATE_TABLES", "0") == "1":
            Base.metadata.create_all(engine)
        _state["sf"] = make_session_factory(engine)
        _state["wallet"] = SettlementWallet.from_url(os.getenv("WALLET_URL", "http://localhost:8001"))
    return _state["sf"], _state["wallet"]


@celery.task(name="settlement.process_pending")
def process_pending_task() -> dict:
    sf, wallet = _deps()
    return vars(process_pending(sf, wallet))
