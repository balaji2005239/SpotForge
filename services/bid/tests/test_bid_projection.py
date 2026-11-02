from bid_svc.main import create_app
from bid_svc.models import Bid, BidStatus
from bid_svc.projection import apply_allocation_event
from bid_svc.wallet_client import WalletClient
from fastapi.testclient import TestClient
from wallet_svc.main import create_app as create_wallet_app


def _setup():
    wallet = TestClient(create_wallet_app("sqlite://"))
    wallet.__enter__()
    wallet.post("/v1/wallets/u1/deposit", json={"amount_micro": 20_000_000, "idempotency_key": "d"})
    bids = TestClient(create_app("sqlite://", wallet=WalletClient(wallet)))
    bids.__enter__()
    r = bids.post("/v1/bids", json={"user_id": "u1", "gpu_type": "A100", "qty": 2,
                                    "max_price_micro": 1_000_000, "duration_s": 600})
    return wallet, bids, r.json()["bid_id"]


def _apply(bids, topic, bid_id):
    with bids.app.state.session_factory() as db:
        out = apply_allocation_event(db, topic, {"bid_id": bid_id})
        db.commit()
        return out


def test_lifecycle_and_terminal_states_stick():
    wallet, bids, bid_id = _setup()
    assert _apply(bids, "allocation.created", bid_id) == "updated"
    assert bids.get(f"/v1/bids/{bid_id}").json()["status"] == "RUNNING"
    assert _apply(bids, "allocation.completed", bid_id) == "updated"
    assert _apply(bids, "allocation.created", bid_id) == "ignored"  # late/out-of-order duplicate
    assert _apply(bids, "allocation.preempted", bid_id) == "ignored"
    assert bids.get(f"/v1/bids/{bid_id}").json()["status"] == "COMPLETED"
    assert bids.delete(f"/v1/bids/{bid_id}").status_code == 409  # can't cancel a finished bid
    assert _apply(bids, "allocation.created", "nope") == "unknown_bid"


def test_running_bid_can_be_cancelled_and_cancelled_stays_cancelled():
    wallet, bids, bid_id = _setup()
    _apply(bids, "allocation.created", bid_id)
    assert bids.delete(f"/v1/bids/{bid_id}").json()["status"] == "CANCELLED"
    assert wallet.get(f"/v1/holds/{bid_id}").json()["status"] == "ACTIVE"  # settlement releases it, behind the charges
    assert _apply(bids, "allocation.completed", bid_id) == "ignored"  # cancelled stays cancelled


def test_model_has_status_constants():
    assert Bid.__tablename__ == "bids" and BidStatus.RUNNING == "RUNNING"
