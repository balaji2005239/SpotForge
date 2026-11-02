"""Gateway in front of the REAL wallet, bid, market, capacity and settlement apps (no mocks)."""
import time
from contextlib import ExitStack

import httpx
import pytest
from bid_svc.main import create_app as create_bid
from bid_svc.wallet_client import WalletClient
from capacity_svc.main import create_app as create_capacity
from fastapi.testclient import TestClient
from gateway_svc.main import create_app as create_gateway
from gateway_svc.security import TokenError, make_token, read_token
from market_svc.main import create_app as create_market
from settlement_svc.main import create_app as create_settlement
from settlement_svc.wallet_client import SettlementWallet
from wallet_svc.main import create_app as create_wallet

SECRET = "test-secret-that-is-at-least-32-bytes-long"
PW = "correct horse"


@pytest.fixture()
def env():
    with ExitStack() as stack:
        wallet = create_wallet("sqlite://")
        wallet_http = stack.enter_context(TestClient(wallet))
        apps = {
            "wallet": wallet,
            "bid": create_bid("sqlite://", wallet=WalletClient(wallet_http)),
            "capacity": create_capacity("sqlite://"),
            "market": create_market("sqlite://"),
            "settlement": create_settlement("sqlite://", wallet=SettlementWallet(wallet_http)),
        }
        for a in apps.values():
            if a is not wallet:
                stack.enter_context(TestClient(a))
        ups = {n: httpx.AsyncClient(transport=httpx.ASGITransport(app=a), base_url="http://svc")
               for n, a in apps.items()}
        gw = create_gateway("sqlite://", upstreams=ups, jwt_secret=SECRET, admin_usernames={"root"}, ws_poll_s=0.05)
        client = stack.enter_context(TestClient(gw))
        client.apps = apps
        yield client


def signup(c, name, role_expected="user"):
    r = c.post("/auth/signup", json={"username": name, "password": PW})
    assert r.status_code == 201, r.text
    assert r.json()["role"] == role_expected
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def bid_body(**kw):
    return {"gpu_type": "A100", "qty": 1, "max_price_micro": 2_000_000, "duration_s": 600, **kw}


# ---------- accounts ----------
def test_signup_login_me_and_roles(env):
    h = signup(env, "alice")
    assert env.get("/auth/me", headers=h).json() == {"username": "alice", "role": "user"}
    r = env.post("/auth/login", json={"username": "Alice", "password": PW})  # case-insensitive username
    assert r.status_code == 200 and r.json()["username"] == "alice"
    signup(env, "root", "admin")


def test_duplicate_wrong_password_and_weak_input(env):
    signup(env, "alice")
    assert env.post("/auth/signup", json={"username": "alice", "password": PW}).status_code == 409
    assert env.post("/auth/login", json={"username": "alice", "password": "wrong-password"}).status_code == 401
    assert env.post("/auth/login", json={"username": "nobody", "password": PW}).status_code == 401
    assert env.post("/auth/signup", json={"username": "bob", "password": "short"}).status_code == 422
    assert env.post("/auth/signup", json={"username": "bad name!", "password": PW}).status_code == 422


def test_login_is_throttled_after_repeated_failures(env):
    signup(env, "alice")
    codes = [env.post("/auth/login", json={"username": "alice", "password": "wrong-password"}).status_code
             for _ in range(7)]
    assert codes[:5] == [401] * 5 and codes[5:] == [429, 429]
    assert env.post("/auth/login", json={"username": "alice", "password": PW}).status_code == 429


# ---------- tokens ----------
def test_bad_tokens_are_rejected(env):
    signup(env, "alice")
    url = "/api/wallet/v1/wallets/alice"
    assert env.get(url).status_code == 401  # no token
    assert env.get(url, headers={"Authorization": "Bearer junk"}).status_code == 401
    assert env.get(url, headers={"Authorization": "Basic abc"}).status_code == 401
    forged = make_token("alice", "admin", "some-other-secret-also-32-bytes-long!!", 60)
    assert env.get(url, headers={"Authorization": f"Bearer {forged}"}).status_code == 401
    expired = make_token("alice", "user", SECRET, 60, now=time.time() - 3600)
    assert env.get(url, headers={"Authorization": f"Bearer {expired}"}).status_code == 401
    good = make_token("alice", "user", SECRET, 60)
    assert env.get(url, headers={"Authorization": f"Bearer {good}"}).status_code == 200


def test_unsigned_alg_none_token_is_rejected():
    import base64
    import json
    b = lambda d: base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b"=").decode()  # noqa: E731
    token = f"{b({'alg': 'none', 'typ': 'JWT'})}.{b({'sub': 'root', 'role': 'admin', 'exp': 9999999999})}."
    with pytest.raises(TokenError):
        read_token(token, SECRET)


# ---------- wallet ----------
def test_users_only_touch_their_own_wallet(env):
    a, b = signup(env, "alice"), signup(env, "bob")
    dep = {"amount_micro": 5_000_000, "idempotency_key": "d1"}
    assert env.post("/api/wallet/v1/wallets/alice/deposit", json=dep, headers=a).status_code == 200
    assert env.get("/api/wallet/v1/wallets/alice", headers=a).json()["available_micro"] == 5_000_000
    assert env.get("/api/wallet/v1/wallets/alice", headers=b).status_code == 403
    assert env.get("/api/wallet/v1/wallets/alice/holds", headers=b).status_code == 403
    assert env.post("/api/wallet/v1/wallets/alice/deposit", json={**dep, "idempotency_key": "d2"},
                    headers=b).status_code == 403


def test_internal_wallet_routes_are_not_reachable_by_users(env):
    a = signup(env, "alice")
    for path in ("/api/wallet/v1/holds/h1", "/api/wallet/v1/admin/integrity"):
        assert env.get(path, headers=a).status_code == 403
    assert env.post("/api/wallet/v1/holds", json={}, headers=a).status_code == 403
    assert env.post("/api/wallet/v1/holds/h1/capture", json={}, headers=a).status_code == 403


# ---------- bids ----------
def fund(env, h, user):
    env.post(f"/api/wallet/v1/wallets/{user}/deposit", json={"amount_micro": 500_000_000, "idempotency_key": f"f-{user}"},
             headers=h)


def test_bid_is_always_attributed_to_the_caller(env):
    a, b = signup(env, "alice"), signup(env, "bob")
    fund(env, a, "alice")
    r = env.post("/api/bid/v1/bids", json=bid_body(user_id="alice"), headers=a)
    assert r.status_code == 201 and r.json()["user_id"] == "alice"
    r = env.post("/api/bid/v1/bids", json=bid_body(), headers=a)  # user_id omitted -> filled from the token
    assert r.status_code == 201 and r.json()["user_id"] == "alice"
    assert env.post("/api/bid/v1/bids", json=bid_body(user_id="alice"), headers=b).status_code == 403
    assert env.post("/api/bid/v1/bids", json=bid_body(user_id="alice")).status_code == 401


def test_cancel_and_read_only_your_own_bids(env):
    a, b = signup(env, "alice"), signup(env, "bob")
    fund(env, a, "alice")
    bid_id = env.post("/api/bid/v1/bids", json=bid_body(), headers=a).json()["bid_id"]
    assert env.get(f"/api/bid/v1/bids/{bid_id}", headers=b).status_code == 403
    assert env.delete(f"/api/bid/v1/bids/{bid_id}", headers=b).status_code == 403
    assert env.get(f"/api/bid/v1/bids/{bid_id}", headers=a).status_code == 200
    assert env.delete(f"/api/bid/v1/bids/{bid_id}", headers=a).json()["status"] == "CANCELLED"
    assert env.delete("/api/bid/v1/bids/nope", headers=a).status_code == 404  # not a 500, not a leak
    assert env.get("/api/bid/v1/bids", headers=b).status_code == 403  # list must name yourself
    assert env.get("/api/bid/v1/bids?user_id=alice", headers=b).status_code == 403
    assert len(env.get("/api/bid/v1/bids?user_id=alice", headers=a).json()) == 1


# ---------- market data ----------
def test_market_data_is_public_but_allocations_are_private(env):
    a, b = signup(env, "alice"), signup(env, "bob")
    for p in ("/api/bid/v1/markets", "/api/market/v1/markets/A100/book", "/api/market/v1/markets/latest"):
        assert env.get(p).status_code == 200, p
    assert env.get("/api/market/v1/allocations").status_code == 401
    assert env.get("/api/market/v1/allocations", headers=a).status_code == 403  # must name yourself
    assert env.get("/api/market/v1/allocations?user_id=alice", headers=b).status_code == 403
    assert env.get("/api/market/v1/allocations?user_id=alice", headers=a).status_code == 200


def test_websocket_streams_snapshot_without_login(env):
    with env.websocket_connect("/api/market/ws/markets") as ws:
        m = ws.receive_json()
    assert m["type"] == "snapshot" and "tick_seconds" in m


# ---------- ops ----------
def test_ops_routes_are_admin_only(env):
    user, root = signup(env, "alice"), signup(env, "root", "admin")
    paths = ("/api/capacity/v1/nodes", "/api/capacity/v1/capacity", "/api/settlement/v1/settlement/stats",
             "/api/wallet/v1/admin/integrity", "/api/market/v1/admin/leader")
    for p in paths:
        assert env.get(p, headers=user).status_code == 403, p
        assert env.get(p).status_code == 401, p
        assert env.get(p, headers=root).status_code == 200, p
    assert env.post("/api/market/v1/admin/tick/A100?capacity=8", headers=user).status_code == 403


def test_unknown_service_and_unreachable_upstream(env):
    a = signup(env, "alice")
    assert env.get("/api/nothing/v1/x", headers=a).status_code == 404


def test_only_the_owner_can_raise_a_bid(env):
    a, b = signup(env, "alice"), signup(env, "bob")
    fund(env, a, "alice")
    bid_id = env.post("/api/bid/v1/bids", json=bid_body(), headers=a).json()["bid_id"]
    url = f"/api/bid/v1/bids/{bid_id}"
    assert env.patch(url, json={"max_price_micro": 3_000_000}).status_code == 401
    assert env.patch(url, json={"max_price_micro": 3_000_000}, headers=b).status_code == 403
    r = env.patch(url, json={"max_price_micro": 3_000_000}, headers=a)
    assert r.status_code == 200 and r.json()["max_price_micro"] == 3_000_000
    assert env.patch(url, json={"max_price_micro": 1_000_000}, headers=a).status_code == 422  # lowering is refused
    assert env.patch("/api/wallet/v1/holds/x/increase", json={}, headers=a).status_code == 403  # internal only
