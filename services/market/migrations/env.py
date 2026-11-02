import market_svc.main  # noqa: F401  (registers every table this service creates at runtime)
from alembic import context
from spotcommon.db import Base
from spotcommon.migrate import run_env

run_env(context.config, Base.metadata, "MARKET_DATABASE_URL")
