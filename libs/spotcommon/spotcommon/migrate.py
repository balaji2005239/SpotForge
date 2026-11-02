"""Alembic plumbing shared by every service. Each service owns `migrations/` (its own history, its own database).

    upgrade_to_head(service_dir, url)   # called at startup when MIGRATE_ON_START=1
    run_env(config, metadata, env_var)  # called from each service's migrations/env.py
"""
from __future__ import annotations

import os
from pathlib import Path

from alembic import command, context
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

LOCK_ID = 727_274  # postgres advisory lock: replicas starting together must not migrate concurrently


def make_config(service_dir: Path, url: str | None = None) -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(Path(service_dir) / "migrations"))
    if url:
        cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    return cfg


def upgrade_to_head(service_dir: Path, url: str) -> None:
    command.upgrade(make_config(service_dir, url), "head")


def run_env(config: Config, metadata, env_var: str) -> None:
    url = config.get_main_option("sqlalchemy.url") or os.environ[env_var]
    if context.is_offline_mode():
        context.configure(url=url, target_metadata=metadata, literal_binds=True)
        with context.begin_transaction():
            context.run_migrations()
        return
    engine = create_engine(url, poolclass=NullPool)
    pg = engine.dialect.name == "postgresql"
    with engine.connect() as conn:
        if pg:
            conn.execute(text("SELECT pg_advisory_lock(:k)"), {"k": LOCK_ID})
        try:
            context.configure(connection=conn, target_metadata=metadata, compare_type=not url.startswith("sqlite"))
            with context.begin_transaction():
                context.run_migrations()
            conn.commit()
        finally:
            if pg:
                conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": LOCK_ID})
                conn.commit()
