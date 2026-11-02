"""Every timestamp the browser receives must carry a zone, even from sqlite (which returns naive datetimes)."""
from datetime import datetime

from fastapi.testclient import TestClient
from market_svc.main import create_app
from spotcommon.timeutil import iso_utc


def parse_aware(s):
    return datetime.fromisoformat(s).tzinfo is not None


def test_iso_utc_handles_naive_and_aware():
    assert iso_utc(datetime(2026, 1, 1, 12, 0)).endswith("+00:00")
    assert iso_utc(datetime.fromisoformat("2026-01-01T17:30:00+05:30")) == "2026-01-01T12:00:00+00:00"


def test_api_timestamps_are_zone_aware():
    from market_svc.events import apply_event
    from spotcommon.money import hold_amount

    with TestClient(create_app("sqlite://")) as c:
        with c.app.state.session_factory() as db:
            for bid_id, seq, price in (("low", 1, 900_000), ("whale", 2, 5_000_000)):
                apply_event(db, "bid.submitted", {
                    "bid_id": bid_id, "seq": seq, "user_id": "u", "gpu_type": "A100", "qty": 8,
                    "max_price_micro": price, "duration_s": 600, "divisible": False,
                    "hold_amount_micro": hold_amount(price, 8, 600, 5)})
                db.commit()
                if bid_id == "low":
                    c.post("/v1/admin/tick/A100", params={"capacity": 8})
        c.post("/v1/admin/tick/A100", params={"capacity": 8})  # low is outbid -> notice (default grace > 0)
        alloc = next(a for a in c.get("/v1/allocations", params={"user_id": "u"}).json() if a["bid_id"] == "low")
        assert alloc["grace_deadline_at"] and parse_aware(alloc["grace_deadline_at"])
        assert parse_aware(c.get("/v1/markets/A100/ticks").json()[0]["at"])
        assert parse_aware(c.get("/v1/markets/latest").json()["markets"]["A100"]["at"])
