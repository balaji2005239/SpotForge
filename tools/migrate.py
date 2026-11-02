"""python tools/migrate.py <service> [revision=head]  (uses <SERVICE>_DATABASE_URL)"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
service, rev = sys.argv[1], (sys.argv[2] if len(sys.argv) > 2 else "head")
sys.path[:0] = [str(ROOT / "libs" / "spotcommon"), str(ROOT / "libs" / "clearing_core"),
                str(ROOT / "services" / service)]

from alembic import command  # noqa: E402
from spotcommon.migrate import make_config  # noqa: E402

command.upgrade(make_config(ROOT / "services" / service, os.environ[f"{service.upper()}_DATABASE_URL"]), rev)
