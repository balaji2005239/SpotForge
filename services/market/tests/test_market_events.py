from market_svc.events import apply_event
from market_svc.models import BookBid, Status
from spotcommon.db import Base, make_engine, make_session_factory


def mkdb():
    e = make_engine("sqlite://")
    Base.metadata.create_all(e)
    return make_session_factory(e)()


def payload(bid_id="b1", seq=1, **kw):
    return {"bid_id": bid_id, "seq": seq, "user_id": "u1", "gpu_type": "A100", "qty": 2,
            "max_price_micro": 1_000_000, "duration_s": 60, "divisible": False, **kw}


def test_submit_is_idempotent():
    db = mkdb()
    assert apply_event(db, "bid.submitted", payload()) == "queued"
    db.commit()
    assert apply_event(db, "bid.submitted", payload()) == "ignored"
    assert db.query(BookBid).count() == 1


def test_cancel_queued_and_duplicate_cancel():
    db = mkdb()
    apply_event(db, "bid.submitted", payload())
    db.commit()
    assert apply_event(db, "bid.cancelled", payload()) == "cancelled"
    assert apply_event(db, "bid.cancelled", payload()) == "ignored"
    assert db.get(BookBid, "b1").status == Status.CANCELLED


def test_cancel_overtaking_submit_leaves_tombstone():
    db = mkdb()
    assert apply_event(db, "bid.cancelled", payload()) == "tombstoned"
    db.commit()
    assert apply_event(db, "bid.submitted", payload()) == "ignored"  # must NOT resurrect the bid
    assert db.get(BookBid, "b1").status == Status.CANCELLED
