from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy.pool import StaticPool


class Base(DeclarativeBase):
    pass


def make_engine(url: str) -> Engine:
    if url.startswith("sqlite"):
        if url in ("sqlite://", "sqlite:///:memory:"):
            # In-memory sqlite (unit tests): one shared connection. NOT safe for concurrent threads.
            return create_engine(url, connect_args={"check_same_thread": False}, poolclass=StaticPool)
        # File-backed sqlite (dev stack): a real connection per session, writers serialized with
        # BEGIN IMMEDIATE (sqlite has no row locks, so this is what makes read-modify-write safe).
        engine = create_engine(url, connect_args={"check_same_thread": False, "timeout": 30})

        @event.listens_for(engine, "connect")
        def _on_connect(dbapi_conn, _record):
            dbapi_conn.isolation_level = None  # we emit BEGIN ourselves (also makes SAVEPOINTs reliable)
            dbapi_conn.execute("PRAGMA journal_mode=WAL")
            dbapi_conn.execute("PRAGMA busy_timeout=30000")

        @event.listens_for(engine, "begin")
        def _on_begin(conn):
            conn.exec_driver_sql("BEGIN IMMEDIATE")

        return engine
    return create_engine(url, pool_pre_ping=True, pool_size=10, max_overflow=20)


def make_session_factory(engine: Engine) -> sessionmaker:
    return sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)


from sqlalchemy import BigInteger, Integer  # noqa: E402

# BIGINT in Postgres; plain INTEGER in sqlite so autoincrement works in tests.
PK_TYPE = BigInteger().with_variant(Integer, "sqlite")
