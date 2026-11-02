"""Autogenerate the next migration for one service: python tools/new_migration.py <service> "what changed".
Upgrades a scratch sqlite database to the current head, diffs it against the models, writes the revision."""
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT / "libs" / "spotcommon"), str(ROOT / "libs" / "clearing_core")]
service, message = sys.argv[1], " ".join(sys.argv[2:]) or "schema change"
sys.path.insert(0, str(ROOT / "services" / service))

import importlib  # noqa: E402

from alembic import command  # noqa: E402
from spotcommon.migrate import make_config  # noqa: E402

importlib.import_module(f"{service}_svc.main")
with tempfile.TemporaryDirectory() as tmp:
    cfg = make_config(ROOT / "services" / service, f"sqlite:///{tmp}/scratch.db")
    command.upgrade(cfg, "head")
    command.revision(cfg, message=message, autogenerate=True)
