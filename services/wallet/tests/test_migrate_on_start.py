"""MIGRATE_ON_START=1: the service builds its schema from its Alembic history (the production path)."""
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, inspect
from wallet_svc.main import create_app


def test_service_boots_from_migrations(tmp_path, monkeypatch):
    url = f"sqlite:///{tmp_path}/wallet.db"
    monkeypatch.setenv("MIGRATE_ON_START", "1")
    with TestClient(create_app(url, auto_create=False)) as c:
        r = c.post("/v1/wallets/u1/deposit", json={"amount_micro": 5_000_000, "idempotency_key": "k1"})
        assert r.status_code == 200
        assert c.get("/v1/wallets/u1").json()["available_micro"] == 5_000_000
    assert "alembic_version" in inspect(create_engine(url)).get_table_names()
    with TestClient(create_app(url, auto_create=False)):  # second boot: already at head, a no-op
        pass
