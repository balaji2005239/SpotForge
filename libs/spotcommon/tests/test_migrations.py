"""Every service's Alembic history must reproduce its models exactly, and downgrade cleanly.
Runs in a subprocess per service because all services share one SQLAlchemy Base inside a single process."""
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]


@pytest.mark.parametrize("service", ["wallet", "bid", "capacity", "market", "settlement", "gateway"])
def test_migrations_match_models(service):
    r = subprocess.run([sys.executable, str(ROOT / "tools" / "check_migrations.py"), service],
                       capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stdout + r.stderr
