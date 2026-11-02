"""Ledger under real thread concurrency (file-backed sqlite, one connection per session).
Regression for the dev-stack incident where a shared connection leaked uncommitted writes between
users. These do NOT replace a Postgres run (row locks are a no-op on sqlite; BEGIN IMMEDIATE serializes)."""
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from spotcommon.db import Base, make_engine, make_session_factory
from wallet_svc import ledger


@pytest.fixture()
def sf(tmp_path):
    engine = make_engine(f"sqlite:///{tmp_path}/wallet.db")
    Base.metadata.create_all(engine)
    return make_session_factory(engine)


def test_concurrent_holds_cannot_overspend(sf):
    with sf() as db:
        ledger.deposit(db, "u1", 1_000_000, "d")
        db.commit()

    def attempt(i):
        with sf() as db:
            try:
                ledger.place_hold(db, "u1", f"h{i}", 300_000)
                db.commit()
                return True
            except ledger.InsufficientFunds:
                db.rollback()
                return False

    with ThreadPoolExecutor(8) as ex:
        results = list(ex.map(attempt, range(24)))
    assert sum(results) == 3  # 1_000_000 // 300_000: never more, never fewer
    with sf() as db:
        assert ledger.balances(db, "u1") == (100_000, 900_000)
        assert ledger.verify_integrity(db) == []


def test_same_idempotency_key_raced_by_many_threads_applies_once(sf):
    with sf() as db:
        ledger.deposit(db, "u1", 1_000_000, "d")
        ledger.place_hold(db, "u1", "h1", 500_000)
        db.commit()

    def capture(_):
        with sf() as db:
            r = ledger.capture(db, "h1", 100_000, "tick-1")
            db.commit()
            return r.replayed

    with ThreadPoolExecutor(12) as ex:
        replayed = list(ex.map(capture, range(24)))
    assert replayed.count(False) == 1 and replayed.count(True) == 23
    with sf() as db:
        assert ledger.balances(db, "u1") == (500_000, 400_000)
        assert ledger.verify_integrity(db) == []


def test_mixed_workload_and_readers_never_see_phantom_money(sf):
    """The original incident: brand-new users must never show a balance while others write."""
    def writer(i):
        for n in range(15):
            u = f"w{i}-{n}"
            with sf() as db:
                ledger.deposit(db, u, 5_000_000, str(uuid.uuid4()))
                db.commit()
            with sf() as db:
                ledger.place_hold(db, u, f"{u}-h", 8_420)
                db.commit()
            with sf() as db:
                ledger.capture(db, f"{u}-h", 1_000, f"{u}-c")
                ledger.release(db, f"{u}-h", f"{u}-r")
                db.commit()

    def reader(i):
        phantom = []
        for n in range(150):
            with sf() as db:
                bal = ledger.balances(db, f"fresh-{i}-{n}")
            if bal != (0, 0):
                phantom.append(bal)
        return phantom

    with ThreadPoolExecutor(10) as ex:
        writers = [ex.submit(writer, i) for i in range(4)]
        readers = [ex.submit(reader, i) for i in range(6)]
        for w in writers:
            w.result()
        assert [p for r in readers for p in r.result()] == []
    with sf() as db:
        assert ledger.verify_integrity(db) == []
