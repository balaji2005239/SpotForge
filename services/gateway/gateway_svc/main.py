"""API gateway: the only public entry point. Sign-up/login, token verification, per-route authorization,
and reverse proxying to the internal services (HTTP) plus the live market stream (WebSocket)."""
from __future__ import annotations

import asyncio
import os
import re
from collections.abc import Iterator
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request, Response, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
from spotcommon.db import Base, make_engine, make_session_factory
from spotcommon.migrate import upgrade_to_head
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .hub import Hub
from .models import User
from .policy import authorize
from .ratelimit import RateLimiter
from .security import DUMMY_HASH, Claims, TokenError, hash_password, make_token, read_token, verify_password

USERNAME = re.compile(r"^[a-z0-9][a-z0-9_-]{2,31}$")
DEV_SECRET = "dev-only-secret-change-me-in-production"
SERVICES = ("wallet", "bid", "capacity", "market", "settlement")


class Credentials(BaseModel):
    username: str = Field(min_length=3, max_length=32)
    password: str = Field(min_length=8, max_length=128)


def upstreams_from_env() -> dict[str, httpx.AsyncClient]:
    urls = {"wallet": "WALLET_URL", "bid": "BID_URL", "capacity": "CAPACITY_URL",
            "market": "MARKET_URL", "settlement": "SETTLEMENT_URL"}
    return {n: httpx.AsyncClient(base_url=os.environ[v], timeout=10.0) for n, v in urls.items() if v in os.environ}


def create_app(database_url: str | None = None, upstreams: dict[str, httpx.AsyncClient] | None = None,
               jwt_secret: str | None = None, token_ttl_s: int = 3600, admin_usernames: set[str] | None = None,
               allow_test_deposit: bool | None = None, auto_create: bool | None = None,
               ws_poll_s: float | None = None, redis_sync=None, redis_async=None,
               bid_rate: tuple[int, int] = (30, 10)) -> FastAPI:
    url = database_url or os.getenv("GATEWAY_DATABASE_URL", "sqlite://")
    engine = make_engine(url)
    Session_ = make_session_factory(engine)
    secret = jwt_secret or os.getenv("JWT_SECRET", DEV_SECRET)
    admins = admin_usernames if admin_usernames is not None else {
        u.strip() for u in os.getenv("ADMIN_USERNAMES", "admin").split(",") if u.strip()}
    deposit_ok = allow_test_deposit if allow_test_deposit is not None else os.getenv("ALLOW_TEST_DEPOSIT", "1") == "1"
    poll = ws_poll_s if ws_poll_s is not None else float(os.getenv("WS_POLL_S", "0.5"))
    ups = upstreams if upstreams is not None else upstreams_from_env()
    if auto_create is None:
        auto_create = os.getenv("AUTO_CREATE_TABLES", "0") == "1" or url.startswith("sqlite")
    redis_url = os.getenv("REDIS_URL")
    if redis_url and redis_sync is None and redis_async is None:
        import redis
        import redis.asyncio as aredis

        redis_sync = redis.Redis.from_url(redis_url, decode_responses=True, socket_timeout=1)
        redis_async = aredis.Redis.from_url(redis_url, decode_responses=True)
    limiter = RateLimiter(redis_sync)

    async def fetch_latest() -> dict:
        r = await ups["market"].get("/v1/markets/latest")
        r.raise_for_status()
        return r.json()

    hub = Hub(fetch_latest, poll, redis_async)
    app_hub = hub

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if secret == DEV_SECRET:
            print("[gateway] WARNING: using the built-in dev JWT secret; set JWT_SECRET", flush=True)
        if os.getenv("MIGRATE_ON_START") == "1":  # production: versioned schema (see migrations/)
            upgrade_to_head(Path(__file__).resolve().parents[1], url)
        elif auto_create:  # dev/test
            Base.metadata.create_all(engine)
        if "market" in ups:
            hub.start()
        yield
        await hub.stop()
        for c in ups.values():
            await c.aclose()

    app = FastAPI(title="API Gateway", version="0.1.0", lifespan=lifespan)
    app.state.session_factory = Session_
    app.state.hub = app_hub
    app.state.limiter = limiter

    def get_db() -> Iterator[Session]:
        with Session_() as db:
            yield db

    def claims_of(request: Request) -> Claims | None:
        h = request.headers.get("authorization", "")
        if not h:
            return None
        scheme, _, token = h.partition(" ")
        if scheme.lower() != "bearer" or not token:
            raise HTTPException(401, "Malformed Authorization header.")
        try:
            return read_token(token, secret)
        except TokenError:
            raise HTTPException(401, "Your session expired. Sign in again.")

    def session_out(u: User) -> dict:
        return {"access_token": make_token(u.username, u.role, secret, token_ttl_s), "token_type": "bearer",
                "expires_in": token_ttl_s, "username": u.username, "role": u.role}

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    @app.post("/auth/signup", status_code=201)
    def signup(c: Credentials, db: Session = Depends(get_db)):
        name = c.username.strip().lower()
        if not USERNAME.match(name):
            raise HTTPException(422, "Usernames are 3-32 characters: letters, numbers, - and _.")
        u = User(username=name, pw_hash=hash_password(c.password), role="admin" if name in admins else "user")
        db.add(u)
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            raise HTTPException(409, "That username is taken.")
        return session_out(u)

    @app.post("/auth/login")
    def login(c: Credentials, db: Session = Depends(get_db)):
        name = c.username.strip().lower()
        fkey = f"login-fail:{name}"
        if limiter.count(fkey, 60) >= 5:
            raise HTTPException(429, "Too many failed attempts. Try again in a minute.")
        u = db.scalar(select(User).where(User.username == name))
        ok = verify_password(c.password, u.pw_hash if u else DUMMY_HASH)
        if not (u and ok):
            limiter.hit(fkey, 60)
            raise HTTPException(401, "Wrong username or password.")
        limiter.reset(fkey)
        return session_out(u)

    @app.get("/auth/me")
    def me(request: Request):
        c = claims_of(request)
        if c is None:
            raise HTTPException(401, "Sign in first.")
        return {"username": c.sub, "role": c.role}

    async def forward(svc: str, request: Request, path: str, body: bytes, claims: Claims | None) -> Response:
        headers = {"content-type": request.headers.get("content-type", "application/json")}
        if claims:
            headers["x-user-id"] = claims.sub  # informational; the Authorization header is NOT passed on
        try:
            r = await ups[svc].request(request.method, path, params=list(request.query_params.multi_items()),
                                       content=body, headers=headers)
        except httpx.HTTPError:
            raise HTTPException(502, f"The {svc} service is unavailable.")
        return Response(r.content, status_code=r.status_code,
                        media_type=r.headers.get("content-type", "application/json"))

    @app.api_route("/api/{svc}/{path:path}", methods=["GET", "POST", "DELETE", "PATCH"])
    async def proxy(svc: str, path: str, request: Request):
        if svc not in SERVICES or svc not in ups:
            raise HTTPException(404, "Unknown service.")
        full = "/" + path
        claims = claims_of(request)
        rule = authorize(svc, request.method, full, claims, dict(request.query_params), deposit_ok)
        bidding = svc == "bid" and request.method in ("POST", "PATCH")
        if bidding and claims and claims.role != "admin" and not limiter.allow(f"bid:{claims.sub}", *bid_rate):
            raise HTTPException(429, "You're bidding too fast. Slow down a little.")
        body = await request.body()
        if rule.inject_user and claims:
            import json
            try:
                data = json.loads(body or b"{}")
            except ValueError:
                raise HTTPException(422, "Body must be JSON.")
            if not isinstance(data, dict):
                raise HTTPException(422, "Body must be a JSON object.")
            if data.get("user_id", claims.sub) != claims.sub:
                raise HTTPException(403, "You can only bid as yourself.")
            data["user_id"] = claims.sub
            body = json.dumps(data).encode()
        if rule.owner_bid_id and claims:
            r = await ups["bid"].get(f"/v1/bids/{rule.owner_bid_id}")
            if r.status_code == 200 and r.json().get("user_id") != claims.sub:
                raise HTTPException(403, "That job belongs to someone else.")
        return await forward(svc, request, full, body, claims)

    @app.websocket("/api/market/ws/markets")
    async def stream(ws: WebSocket):
        """Public market feed: a snapshot on connect, then one message per new tick per market (see hub.py)."""
        await ws.accept()
        q = hub.subscribe()
        try:
            await ws.send_json(await hub.snapshot())

            async def pump():
                while True:
                    await ws.send_json(await q.get())

            async def watch():  # notices the client leaving without waiting for the next tick
                while True:
                    await ws.receive_text()

            tasks = {asyncio.create_task(pump()), asyncio.create_task(watch())}
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for t in pending:
                t.cancel()
            for t in done:
                t.result()
        except (WebSocketDisconnect, RuntimeError, httpx.HTTPError):
            return
        finally:
            hub.unsubscribe(q)

    return app


app = create_app() if os.getenv("GATEWAY_DATABASE_URL") else None
