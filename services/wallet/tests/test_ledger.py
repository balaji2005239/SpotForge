import pytest
from fastapi.testclient import TestClient
from spotcommon.db import Base, make_engine, make_session_factory
from sqlalchemy.orm import Session
from wallet_svc import ledger
from wallet_svc.main import create_app


@pytest.fixture()
def db():
    engine = make_engine("sqlite://")
    Base.metadata.create_all(engine)
    with make_session_factory(engine)() as s:
        yield s


def fund(db: Session, user="u1", amount=1_000_000, key="d1"):
    ledger.deposit(db, user, amount, key)
    db.commit()


def test_deposit_hold_capture_release_flow(db):
    fund(db, amount=1_000_000)
    ledger.place_hold(db, "u1", "bid1", 600_000)
    assert ledger.balances(db, "u1") == (400_000, 600_000)
    ledger.capture(db, "bid1", 100_000, "cap:bid1:t1")
    ledger.capture(db, "bid1", 150_000, "cap:bid1:t2")
    _, refunded = ledger.release(db, "bid1", "rel:bid1")
    db.commit()
    assert refunded == 350_000
    assert ledger.balances(db, "u1") == (750_000, 0)
    assert ledger.verify_integrity(db) == []


def test_cannot_overspend(db):
    fund(db, amount=500_000)
    with pytest.raises(ledger.InsufficientFunds):
        ledger.place_hold(db, "u1", "bid1", 500_001)


def test_hold_and_capture_are_idempotent(db):
    fund(db)
    a = ledger.place_hold(db, "u1", "bid1", 300_000)
    b = ledger.place_hold(db, "u1", "bid1", 300_000)
    assert (a.replayed, b.replayed) == (False, True) and a.txn_id == b.txn_id
    c1 = ledger.capture(db, "bid1", 50_000, "k1")
    c2 = ledger.capture(db, "bid1", 50_000, "k1")  # retry / duplicate event
    assert c2.replayed and c1.txn_id == c2.txn_id
    assert ledger.balances(db, "u1") == (700_000, 250_000)
    assert ledger.verify_integrity(db) == []


def test_idempotency_key_reuse_with_different_amount_conflicts(db):
    fund(db)
    ledger.place_hold(db, "u1", "bid1", 300_000)
    ledger.capture(db, "bid1", 50_000, "k1")
    with pytest.raises(ledger.Conflict):
        ledger.capture(db, "bid1", 60_000, "k1")


def test_capture_cannot_exceed_hold(db):
    fund(db)
    ledger.place_hold(db, "u1", "bid1", 100_000)
    ledger.capture(db, "bid1", 90_000, "k1")
    with pytest.raises(ledger.Conflict):
        ledger.capture(db, "bid1", 20_000, "k2")


def test_release_is_idempotent_and_closes_hold(db):
    fund(db)
    ledger.place_hold(db, "u1", "bid1", 100_000)
    r1, ref1 = ledger.release(db, "bid1", "rel")
    r2, ref2 = ledger.release(db, "bid1", "rel")
    assert r2.replayed and ref1 == ref2 == 100_000
    with pytest.raises(ledger.Conflict):
        ledger.capture(db, "bid1", 1, "late")  # no captures after close
    assert ledger.balances(db, "u1") == (1_000_000, 0)


def test_full_capture_closes_hold_and_replay_still_works(db):
    fund(db)
    ledger.place_hold(db, "u1", "bid1", 100_000)
    ledger.capture(db, "bid1", 100_000, "k1")
    assert ledger.capture(db, "bid1", 100_000, "k1").replayed  # replay after close is fine
    with pytest.raises(ledger.Conflict):
        ledger.release(db, "bid1", "rel")  # nothing left


def test_unknown_hold(db):
    with pytest.raises(ledger.NotFound):
        ledger.capture(db, "nope", 1, "k")


def test_integrity_detects_tampering(db):
    fund(db)
    acct = db.get(ledger.Account, ledger.avail("u1"))
    acct.balance += 5  # simulate drift
    db.commit()
    assert any("cached" in p for p in ledger.verify_integrity(db))


# ---- HTTP layer ----
def test_http_flow_and_status_codes():
    c = TestClient(create_app("sqlite://"))
    with c:
        assert c.post("/v1/wallets/u1/deposit", json={"amount_micro": 1_000_000, "idempotency_key": "d1"}).status_code == 200
        r = c.post("/v1/holds", json={"hold_id": "b1", "user_id": "u1", "amount_micro": 400_000})
        assert r.status_code == 201
        assert c.post("/v1/holds", json={"hold_id": "b2", "user_id": "u1", "amount_micro": 700_000}).status_code == 402
        assert c.post("/v1/holds/b1/capture", json={"amount_micro": 100_000, "idempotency_key": "t1"}).json()["replayed"] is False
        assert c.post("/v1/holds/b1/capture", json={"amount_micro": 100_000, "idempotency_key": "t1"}).json()["replayed"] is True
        assert c.post("/v1/holds/b1/capture", json={"amount_micro": 999_999, "idempotency_key": "t2"}).status_code == 409
        rel = c.post("/v1/holds/b1/release", json={"idempotency_key": "r1"}).json()
        assert rel["refunded_micro"] == 300_000
        w = c.get("/v1/wallets/u1").json()
        assert (w["available_micro"], w["held_micro"]) == (900_000, 0)
        assert c.get("/v1/holds/nope").status_code == 404
        assert c.get("/v1/admin/integrity").json() == {"ok": True, "problems": []}


def test_user_holds_listing():
    c = TestClient(create_app("sqlite://"))
    with c:
        c.post("/v1/wallets/u1/deposit", json={"amount_micro": 1_000_000, "idempotency_key": "d"})
        c.post("/v1/holds", json={"hold_id": "b1", "user_id": "u1", "amount_micro": 100_000})
        c.post("/v1/holds", json={"hold_id": "b2", "user_id": "u1", "amount_micro": 200_000})
        c.post("/v1/holds/b1/capture", json={"amount_micro": 30_000, "idempotency_key": "k"})
        rows = c.get("/v1/wallets/u1/holds").json()
        assert {r["hold_id"]: r["captured_micro"] for r in rows} == {"b1": 30_000, "b2": 0}
        assert c.get("/v1/wallets/nobody/holds").json() == []
