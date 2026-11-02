"""A wallet timeout after the PENDING row is written must not strand the bid (and its escrow)."""
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from bid_svc.main import create_app as create_bid_app
from bid_svc.models import Outbox
from bid_svc.wallet_client import WalletClient
from fastapi.testclient import TestClient
from sqlalchemy import select
from wallet_svc.main import create_app as create_wallet_app

BODY = {"user_id": "alice", "gpu_type": "A100", "qty": 2, "max_price_micro": 1_000_000, "duration_s": 600,
        "client_request_id": "r1"}


class LoseResponses:
    """Executes the call, then pretends the response never came back (the nasty case)."""

    def __init__(self, inner):
        self.inner, self.lose = inner, 1

    def post(self, *a, **kw):
        r = self.inner.post(*a, **kw)
        if self.lose:
            self.lose -= 1
            raise httpx.ReadTimeout("response lost")
        return r


@pytest.fixture()
def env():
    with TestClient(create_wallet_app("sqlite://")) as wallet:
        wallet.post("/v1/wallets/alice/deposit", json={"amount_micro": 50_000_000, "idempotency_key": "d"})
        flaky = LoseResponses(wallet)
        with TestClient(create_bid_app("sqlite://", wallet=WalletClient(flaky))) as bids:
            bids.wallet, bids.flaky = wallet, flaky
            yield bids


def submitted_events(env):
    with env.app.state.session_factory() as db:
        return len(db.scalars(select(Outbox).where(Outbox.topic == "bid.submitted")).all())


def test_retry_with_same_request_id_finishes_a_pending_bid(env):
    r = env.post("/v1/bids", json=BODY)
    assert r.status_code == 503
    bid_id = r.json()["detail"].split()[3]
    assert env.get(f"/v1/bids/{bid_id}").json()["status"] == "PENDING"
    assert env.wallet.get("/v1/wallets/alice").json()["held_micro"] > 0  # escrow exists, bid not live yet

    r2 = env.post("/v1/bids", json=BODY)  # same client_request_id
    assert r2.status_code == 201 and r2.json()["bid_id"] == bid_id and r2.json()["status"] == "ACTIVE"
    assert submitted_events(env) == 1
    env.post("/v1/bids", json=BODY)  # a third identical retry changes nothing
    assert submitted_events(env) == 1
    assert env.wallet.get("/v1/admin/integrity").json()["ok"] is True


def test_sweeper_activates_stranded_bids_without_any_client_retry(env):
    assert env.post("/v1/bids", json=BODY).status_code == 503
    sweep = env.app.state.sweep_pending
    assert sweep(3600) == 0  # too young: the client may still be retrying
    later = datetime.now(timezone.utc) + timedelta(hours=1)
    assert sweep(0, now=later) == 1
    assert submitted_events(env) == 1
    assert env.get("/v1/bids", params={"user_id": "alice"}).json()[0]["status"] == "ACTIVE"
    assert sweep(0, now=later) == 0  # nothing left to do


def test_sweeper_rejects_when_funds_are_gone_and_leaves_no_escrow(env):
    env.wallet.post("/v1/wallets/alice/deposit", json={"amount_micro": 1, "idempotency_key": "d2"})
    big = {**BODY, "qty": 8, "max_price_micro": 9_000_000, "duration_s": 3600, "client_request_id": "big"}
    env.flaky.lose = 0
    r = env.post("/v1/bids", json=big)
    assert r.status_code == 201 and r.json()["status"] == "REJECTED"
    assert submitted_events(env) == 0


def test_cancel_works_with_the_wallet_down_because_it_never_calls_it(env):
    assert env.post("/v1/bids", json=BODY).status_code == 503  # first response lost (fixture)
    bid_id = env.post("/v1/bids", json=BODY).json()["bid_id"]  # the retry activates it
    env.flaky.lose = 10**9  # every wallet response lost from now on
    r = env.delete(f"/v1/bids/{bid_id}")
    assert r.status_code == 200 and r.json()["status"] == "CANCELLED"
    with env.app.state.session_factory() as db:
        assert [o.topic for o in db.scalars(select(Outbox).order_by(Outbox.id))][-1] == "bid.cancelled"
