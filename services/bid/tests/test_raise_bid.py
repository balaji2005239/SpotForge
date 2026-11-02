"""PATCH /v1/bids/{id}: raise a live bid's max price (escrow grows first, then bid + event commit together)."""
import httpx
import pytest
from bid_svc.main import create_app as create_bid_app
from bid_svc.models import Outbox
from bid_svc.wallet_client import WalletClient
from fastapi.testclient import TestClient
from spotcommon.money import hold_amount
from sqlalchemy import select
from wallet_svc.main import create_app as create_wallet_app

RESERVE = 800_000
BODY = {"user_id": "alice", "gpu_type": "A100", "qty": 4, "max_price_micro": 1_000_000, "duration_s": 600}


@pytest.fixture()
def env():
    with TestClient(create_wallet_app("sqlite://")) as wallet:
        wallet.post("/v1/wallets/alice/deposit", json={"amount_micro": 100_000_000, "idempotency_key": "d"})
        with TestClient(create_bid_app("sqlite://", wallet=WalletClient(wallet))) as bids:
            bids.wallet = wallet
            yield bids


def place(env, **kw):
    return env.post("/v1/bids", json={**BODY, **kw}).json()


def hold_of(env, bid_id):
    return env.wallet.get(f"/v1/holds/{bid_id}").json()


def events(env, topic):
    with env.app.state.session_factory() as db:
        return [o.payload for o in db.scalars(select(Outbox).where(Outbox.topic == topic).order_by(Outbox.id))]


def test_raise_grows_escrow_and_emits_a_versioned_event(env):
    b = place(env)
    before = hold_of(env, b["bid_id"])["amount_micro"]
    r = env.patch(f"/v1/bids/{b['bid_id']}", json={"max_price_micro": 2_500_000})
    assert r.status_code == 200 and r.json()["max_price_micro"] == 2_500_000
    new_hold = hold_amount(2_500_000, 4, 600, 5)
    assert r.json()["hold_amount_micro"] == new_hold > before
    h = hold_of(env, b["bid_id"])
    assert h["amount_micro"] == new_hold and h["remaining_micro"] == new_hold
    w = env.wallet.get("/v1/wallets/alice").json()
    assert w["held_micro"] == new_hold and w["available_micro"] + w["held_micro"] == 100_000_000
    ev = events(env, "bid.repriced")
    assert len(ev) == 1 and ev[0]["max_price_micro"] == 2_500_000 and ev[0]["price_version"] == 1
    assert ev[0]["seq"] and ev[0]["qty"] == 4  # full bid state, so the market can build the row if it overtook the submit
    assert env.wallet.get("/v1/admin/integrity").json()["ok"] is True


def test_successive_raises_bump_the_version(env):
    b = place(env)
    for price, v in ((1_200_000, 1), (1_500_000, 2), (1_500_001, 3)):
        assert env.patch(f"/v1/bids/{b['bid_id']}", json={"max_price_micro": price}).status_code == 200
        assert events(env, "bid.repriced")[-1]["price_version"] == v


def test_same_price_is_a_noop_and_lowering_is_refused(env):
    b = place(env)
    url = f"/v1/bids/{b['bid_id']}"
    assert env.patch(url, json={"max_price_micro": BODY["max_price_micro"]}).status_code == 200
    assert events(env, "bid.repriced") == []
    r = env.patch(url, json={"max_price_micro": 900_000})
    assert r.status_code == 422 and "only raise" in r.json()["detail"]


def test_not_enough_money_changes_nothing(env):
    env.wallet.post("/v1/wallets/poor/deposit", json={"amount_micro": 1_000_000, "idempotency_key": "p"})
    b = place(env, user_id="poor", qty=1, duration_s=60)
    held_before = env.wallet.get("/v1/wallets/poor").json()
    r = env.patch(f"/v1/bids/{b['bid_id']}", json={"max_price_micro": 900_000_000})
    assert r.status_code == 402
    assert env.wallet.get("/v1/wallets/poor").json() == held_before
    assert env.get(f"/v1/bids/{b['bid_id']}").json()["max_price_micro"] == BODY["max_price_micro"]
    assert events(env, "bid.repriced") == []


def test_only_live_bids_can_be_raised(env):
    b = place(env)
    env.delete(f"/v1/bids/{b['bid_id']}")
    assert env.patch(f"/v1/bids/{b['bid_id']}", json={"max_price_micro": 2_000_000}).status_code == 409
    assert env.patch("/v1/bids/nope", json={"max_price_micro": 2_000_000}).status_code == 404


def test_wallet_outage_leaves_the_bid_untouched(env):
    from bid_svc.models import Bid, BidStatus
    from bid_svc.wallet_client import WalletUnavailable

    class Down(WalletClient):
        def increase_hold(self, *a, **k):
            raise WalletUnavailable("down")

    with TestClient(create_bid_app("sqlite://", wallet=Down(httpx.Client()))) as c:
        with c.app.state.session_factory() as db:
            db.add(Bid(bid_id="b1", user_id="alice", gpu_type="A100", qty=1, max_price_micro=1_000_000,
                       duration_s=60, hold_amount_micro=1, status=BidStatus.ACTIVE))
            db.commit()
        assert c.patch("/v1/bids/b1", json={"max_price_micro": 2_000_000}).status_code == 503
        assert c.get("/v1/bids/b1").json()["max_price_micro"] == 1_000_000
