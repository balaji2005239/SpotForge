"""CI guard: python tools/check_migrations.py <service>
Fails if (a) upgrade-to-head doesn't reproduce the models (someone changed a model without a migration),
or (b) downgrade-to-base doesn't remove everything it created."""
import importlib
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
service = sys.argv[1]
sys.path[:0] = [str(ROOT / "libs" / "spotcommon"), str(ROOT / "libs" / "clearing_core"),
                str(ROOT / "services" / service)]

from alembic import command  # noqa: E402
from alembic.autogenerate import compare_metadata  # noqa: E402
from alembic.migration import MigrationContext  # noqa: E402
from spotcommon.db import Base  # noqa: E402
from spotcommon.migrate import make_config  # noqa: E402
from sqlalchemy import create_engine, inspect  # noqa: E402

importlib.import_module(f"{service}_svc.main")
with tempfile.TemporaryDirectory() as tmp:
    url = f"sqlite:///{tmp}/check.db"
    cfg = make_config(ROOT / "services" / service, url)
    command.upgrade(cfg, "head")
    engine = create_engine(url)
    with engine.connect() as conn:
        diff = compare_metadata(MigrationContext.configure(conn), Base.metadata)
    if diff:
        sys.exit(f"{service}: models differ from migrations: {diff}")
    command.downgrade(cfg, "base")
    left = set(inspect(engine).get_table_names()) - {"alembic_version"}
    if left:
        sys.exit(f"{service}: downgrade left tables behind: {sorted(left)}")
print(f"{service}: migrations match models, downgrade is clean")
