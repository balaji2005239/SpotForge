import pytest
from spotcommon.db import Base, make_engine, make_session_factory
from wallet_svc import ledger


@pytest.fixture()
def db():
    e = make_engine("sqlite://")
    Base.metadata.create_all(e)
    with make_session_factory(e)() as s:
        yield s


def setup(db, balance=1_000_000, hold=400_000):
    ledger.deposit(db, "u", balance, "d")
    ledger.place_hold(db, "u", "h", hold)
    db.commit()


def test_increase_moves_money_from_available_to_held_and_grows_the_hold(db):
    setup(db)
    r = ledger.increase_hold(db, "h", 250_000, "inc-1")
    db.commit()
    assert not r.replayed
    assert ledger.balances(db, "u") == (350_000, 650_000)
    assert db.get(ledger.Hold, "h").amount == 650_000
    assert ledger.verify_integrity(db) == []


def test_increase_is_idempotent(db):
    setup(db)
    ledger.increase_hold(db, "h", 250_000, "inc-1")
    assert ledger.increase_hold(db, "h", 250_000, "inc-1").replayed
    db.commit()
    assert ledger.balances(db, "u") == (350_000, 650_000)  # applied once
    with pytest.raises(ledger.Conflict):  # same key, different amount
        ledger.increase_hold(db, "h", 999, "inc-1")


def test_increase_needs_funds_and_leaves_everything_unchanged(db):
    setup(db, balance=500_000, hold=400_000)  # 100_000 available
    with pytest.raises(ledger.InsufficientFunds):
        ledger.increase_hold(db, "h", 150_000, "inc-1")
    db.rollback()
    assert ledger.balances(db, "u") == (100_000, 400_000)
    assert db.get(ledger.Hold, "h").amount == 400_000


def test_cannot_increase_a_closed_hold_and_bad_inputs_are_rejected(db):
    setup(db)
    ledger.release(db, "h", "r")
    with pytest.raises(ledger.Conflict):
        ledger.increase_hold(db, "h", 10, "inc-1")
    with pytest.raises(ledger.LedgerError):
        ledger.increase_hold(db, "h", 0, "inc-2")
    with pytest.raises(ledger.NotFound):
        ledger.increase_hold(db, "nope", 10, "inc-3")


def test_captures_still_bounded_by_the_grown_hold_and_release_refunds_the_rest(db):
    setup(db)
    ledger.increase_hold(db, "h", 100_000, "inc-1")  # 500_000 total
    ledger.capture(db, "h", 450_000, "c1")
    with pytest.raises(ledger.Conflict):
        ledger.capture(db, "h", 60_000, "c2")  # only 50_000 left
    _, refunded = ledger.release(db, "h", "r")
    db.commit()
    assert refunded == 50_000
    assert ledger.balances(db, "u") == (1_000_000 - 450_000, 0)
    assert ledger.verify_integrity(db) == []
