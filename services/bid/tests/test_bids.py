import httpx
import pytest
from bid_svc.main import create_app as create_bid_app
from bid_svc.models import Outbox
from bid_svc.wallet_client import WalletClient
from fastapi.testclient import TestClient
from sqlalchemy import select
from wallet_svc.main import create_app as create_wallet_app


class Env:
    """Real bid service wired to the real wallet service in-process (contract test, no mocks)."""

    def __init__(self):
        self.wallet_app = create_wallet_app("sqlite://")
        self.wallet_http = TestClient(self.wallet_app)
        self.wallet_http.__enter__()
        self.bid_app = create_bid_app("sqlite://", wallet=WalletClient(self.wallet_http))
        self.bids = TestClient(self.bid_app)
        self.bids.__enter__()

    def fund(self, user, micro):
        self.wallet_http.post(f"/v1/wallets/{user}/deposit", json={"amount_micro": micro, "idempotency_key": f"f-{user}"})

    def wallet(self, user):
        return self.wallet_http.get(f"/v1/wallets/{user}").json()

    def close(self):
        self.bids.__exit__(None, None, None)
        self.wallet_http.__exit__(None, None, None)


@pytest.fixture()
def env():
    e = Env()
    yield e
    e.close()


BID = dict(user_id="u1", gpu_type="A100", qty=2, max_price_micro=1_000_000, duration_s=3600)


def test_submit_holds_escrow_and_emits_event(env):
    env.fund("u1", 10_000_000)
    r = env.bids.post("/v1/bids", json=BID)
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "ACTIVE"
    assert body["hold_amount_micro"] == 2_000_160  # 720 ticks * ceil(1.0 USD/hr * 2 gpus * 5s)
    assert env.wallet("u1") == {"user_id": "u1", "available_micro": 7_999_840, "held_micro": 2_000_160}
    with env.bid_app.state.session_factory() as db:
        ev = db.scalars(select(Outbox)).all()
        assert [e.topic for e in ev] == ["bid.submitted"] and ev[0].key == "A100"


def test_insufficient_funds_rejects_without_event(env):
    env.fund("u1", 1_000_000)
    body = env.bids.post("/v1/bids", json=BID).json()
    assert body["status"] == "REJECTED"
    assert env.wallet("u1")["held_micro"] == 0
    with env.bid_app.state.session_factory() as db:
        assert db.scalars(select(Outbox)).all() == []


def test_cancel_is_idempotent_emits_one_event_and_leaves_the_refund_to_settlement(env):
    env.fund("u1", 10_000_000)
    bid_id = env.bids.post("/v1/bids", json=BID).json()["bid_id"]
    held = env.wallet("u1")["held_micro"]
    assert env.bids.delete(f"/v1/bids/{bid_id}").json()["status"] == "CANCELLED"
    assert env.bids.delete(f"/v1/bids/{bid_id}").json()["status"] == "CANCELLED"
    with env.bid_app.state.session_factory() as db:
        assert [o.topic for o in db.scalars(select(Outbox)) if o.topic == "bid.cancelled"] == ["bid.cancelled"]
    assert env.wallet("u1")["held_micro"] == held  # the escrow stays until the market's last charge is settled


def test_client_request_id_makes_submit_idempotent(env):
    env.fund("u1", 10_000_000)
    a = env.bids.post("/v1/bids", json={**BID, "client_request_id": "req-1"}).json()
    b = env.bids.post("/v1/bids", json={**BID, "client_request_id": "req-1"}).json()
    assert a["bid_id"] == b["bid_id"]
    assert env.wallet("u1")["held_micro"] == 2_000_160  # escrowed once


def test_validation(env):
    assert env.bids.post("/v1/bids", json={**BID, "gpu_type": "RTX9090"}).status_code == 422
    assert env.bids.post("/v1/bids", json={**BID, "qty": 0}).status_code == 422
    assert env.bids.post("/v1/bids", json={**BID, "max_price_micro": 1}).status_code == 422  # < reserve


def test_wallet_down_leaves_pending_and_retry_recovers():
    wallet_app = create_wallet_app("sqlite://")
    up = {"ok": False}

    def handler(request: httpx.Request) -> httpx.Response:
        if not up["ok"]:
            raise httpx.ConnectError("wallet down")
        return real.handle_request(request)

    with TestClient(wallet_app) as w:
        w.post("/v1/wallets/u1/deposit", json={"amount_micro": 10_000_000, "idempotency_key": "f"})
        real = w._transport
        flaky = httpx.Client(transport=httpx.MockTransport(handler), base_url="http://testserver")
        with TestClient(create_bid_app("sqlite://", wallet=WalletClient(flaky))) as c:
            r = c.post("/v1/bids", json={**BID, "client_request_id": "r1"})
            assert r.status_code == 503
            pending = c.get("/v1/bids", params={"status": "PENDING"}).json()
            assert len(pending) == 1  # durable, not lost
            up["ok"] = True
            # Client retries the same request -> returns the stored PENDING bid (sweeper activates it).
            assert c.post("/v1/bids", json={**BID, "client_request_id": "r1"}).json()["bid_id"] == pending[0]["bid_id"]


def test_money_flow_is_conserved(env):
    env.fund("u1", 10_000_000)
    for i in range(3):
        env.bids.post("/v1/bids", json={**BID, "client_request_id": f"r{i}"})
    assert env.wallet_http.get("/v1/admin/integrity").json()["ok"] is True
    w = env.wallet("u1")
    assert w["available_micro"] + w["held_micro"] == 10_000_000
