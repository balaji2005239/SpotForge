from datetime import datetime, timedelta, timezone

from capacity_svc import service
from capacity_svc.main import create_app
from fastapi.testclient import TestClient
from spotcommon.db import Base, make_engine, make_session_factory
from spotcommon.outbox import Outbox
from sqlalchemy import select

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def mkdb():
    e = make_engine("sqlite://")
    Base.metadata.create_all(e)
    return make_session_factory(e)()


def test_capacity_counts_only_up_nodes():
    db = mkdb()
    service.register(db, "n1", "A100", 8, T0)
    service.register(db, "n2", "A100", 8, T0)
    service.register(db, "n3", "T4", 4, T0)
    service.mark_failed(db, "n2", "test")
    cap = service.capacity(db, ("T4", "A100", "H100"))
    assert cap["A100"] == {"gpus": 8, "nodes": 1, "failed_nodes": 1}
    assert cap["T4"]["gpus"] == 4 and cap["H100"]["gpus"] == 0


def test_sweep_fails_only_stale_nodes_and_emits_event():
    db = mkdb()
    service.register(db, "stale", "A100", 8, T0)
    service.register(db, "fresh", "A100", 8, T0)
    service.heartbeat(db, "fresh", T0 + timedelta(seconds=20))
    failed = service.sweep(db, T0 + timedelta(seconds=25), timeout_s=15)
    assert failed == ["stale"]
    assert service.sweep(db, T0 + timedelta(seconds=26), timeout_s=15) == []  # idempotent
    db.commit()
    topics = [o.topic for o in db.scalars(select(Outbox))]
    assert topics.count("node.failed") == 1


def test_failed_node_revives_on_heartbeat():
    db = mkdb()
    service.register(db, "n1", "H100", 8, T0)
    service.mark_failed(db, "n1", "x")
    assert service.heartbeat(db, "n1", T0) is True
    assert service.capacity(db, ("H100",))["H100"]["gpus"] == 8
    assert service.heartbeat(db, "ghost", T0) is None


def test_http_api():
    with TestClient(create_app("sqlite://")) as c:
        assert c.post("/v1/nodes/register", json={"node_id": "n1", "gpu_type": "H100", "gpus": 8}).status_code == 201
        assert c.post("/v1/nodes/register", json={"node_id": "n2", "gpu_type": "nope", "gpus": 8}).status_code == 422
        assert c.post("/v1/nodes/n1/heartbeat").json()["revived"] is False
        assert c.post("/v1/nodes/ghost/heartbeat").status_code == 404
        assert c.get("/v1/capacity").json()["H100"]["gpus"] == 8
        assert c.post("/v1/nodes/n1/fail").status_code == 200
        assert c.get("/v1/capacity").json()["H100"]["gpus"] == 0


def test_node_list_endpoint():
    with TestClient(create_app("sqlite://")) as c:
        c.post("/v1/nodes/register", json={"node_id": "n1", "gpu_type": "T4", "gpus": 4})
        c.post("/v1/nodes/register", json={"node_id": "n0", "gpu_type": "A100", "gpus": 8})
        c.post("/v1/nodes/n1/fail")
        nodes = c.get("/v1/nodes").json()
        assert [(n["node_id"], n["status"]) for n in nodes] == [("n0", "UP"), ("n1", "FAILED")]


def test_chaos_crash_ignores_heartbeats_until_down_window_ends():
    db = mkdb()
    service.register(db, "n1", "A100", 8, T0)
    assert service.mark_failed(db, "n1", "chaos", down_s=20, now=T0)
    assert service.heartbeat(db, "n1", T0 + timedelta(seconds=5)) is False  # lost
    assert service.capacity(db, ("A100",))["A100"]["gpus"] == 0
    assert service.heartbeat(db, "n1", T0 + timedelta(seconds=21)) is True  # rejoined
    assert service.capacity(db, ("A100",))["A100"]["gpus"] == 8
