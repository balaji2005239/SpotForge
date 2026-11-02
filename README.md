# Real-Time GPU Spot Market for ML Jobs

Event-driven marketplace where ML jobs bid for GPU capacity. Every tick the engine clears a
uniform-price auction per GPU type, allocates winners, charges the clearing price, and preempts
jobs whose bid falls below the market. See `docs/market-rules.md`.

## Status
| Phase | Scope | State |
|---|---|---|
| 1 | Wallet/ledger, bid service, schema | done |
| 2 | Clearing engine, outbox/relay, capacity service, market tick loop, node simulator | done (Kafka/Postgres paths untested locally, see below) |
| 3 | Settlement (Celery): per-tick captures + refunds from `market.cleared`, retries, dead-letter, reconciliation | done, money loop closed |
| 4+ | Preempt warning window, leader election + fencing, API gateway/auth, React UI, chaos + benchmarks | todo |

## Layout
```
libs/spotcommon     money (integer micro-USD), db helpers, outbox + relay, event bus (Kafka / in-memory)
libs/clearing_core  pure deterministic uniform-price auction
services/wallet     double-entry ledger, escrow holds, idempotent capture/release   :8001
services/bid        bid intake, escrow via wallet, outbox -> bid.submitted/cancelled :8002
services/capacity   node registry, heartbeats, failure sweep, GET /v1/capacity       :8003
services/market     book projection, 5s tick loop, lifecycle, price/book/allocation API :8004
services/settlement market.cleared -> idempotent wallet captures/refunds (Celery + beat), reconcile :8005
services/worker_sim simulated GPU nodes (register, heartbeat, optional crashes)
```

## Event flow
```
bid -> outbox -> Kafka bid.submitted/cancelled -> market (book projection)
capacity (live GPUs) --HTTP--> market tick -> clear() -> market.cleared + allocation.* -> outbox -> Kafka
```
`market.cleared` carries per-bid `charge_micro` and a `charge_key` (`bid_id:snapshot_id`), which
settlement uses as the wallet idempotency key.

## Money guarantees (all covered by tests)
- Escrow is sized so per-tick charges can never exceed it (property-tested).
- Settlement ingests only `market.cleared`, so a refund can never overtake the last charge of a bid.
- Duplicate Kafka delivery or a double-run worker cannot double-charge (unique item key + wallet idempotency).
- Wallet outage: retry with backoff; a bid's refund waits for its earlier charges; exhausted retries go DEAD
  (`GET :8005/v1/settlement/dead`) and keep blocking that bid's refund until an operator resolves it.
- `POST :8005/v1/admin/reconcile` compares settlement's view with the wallet per bid;
  `GET :8001/v1/admin/integrity` re-verifies the whole double-entry ledger.

## Run
```bash
make venv && make test      # 161 tests: engine properties, ledger, money invariants, bid<->wallet, event path
make up                     # docker compose: infra + wallet + bid
```
Smoke test:
```bash
curl -X POST localhost:8001/v1/wallets/alice/deposit -H 'content-type: application/json' \
  -d '{"amount_micro": 50000000, "idempotency_key": "d1"}'
curl -X POST localhost:8002/v1/bids -H 'content-type: application/json' \
  -d '{"user_id":"alice","gpu_type":"A100","qty":2,"max_price_micro":1200000,"duration_s":3600}'
# within ~5s the market clears: price, book and your allocation
curl localhost:8004/v1/markets/A100/price
curl localhost:8004/v1/markets/A100/ticks?limit=5
curl "localhost:8004/v1/allocations?user_id=alice"
curl localhost:8003/v1/capacity
curl localhost:8005/v1/settlement/stats          # charges/refunds flowing
curl -X POST localhost:8005/v1/admin/reconcile   # {"ok": true}
curl localhost:8001/v1/admin/integrity
```
Outbid a running job: submit a second, higher bid for the whole A100 capacity (16 GPUs) and watch
the first become `PREEMPTED`. Kill a node with `curl -X POST localhost:8003/v1/nodes/a100-1/fail`.

> Verification status: the unit/property/chaos tests run on in-memory SQLite plus an in-memory event bus. Separately,
> the full `docker-compose.yml` stack (all Dockerfiles built, 11 containers: Postgres 16, Redis 7, Kafka 3.8, the six
> services, the Celery worker+beat and the nginx UI) was run in a sandbox: migrations on Postgres, a bid through Kafka
> to RUNNING then COMPLETED, Celery settling it to the micro-dollar, a cancel refund, clean integrity/reconcile and no
> tracebacks (see [docs/BENCHMARKS.md](docs/BENCHMARKS.md) section 3). The sandbox needed a CA-trust workaround for
> pip/npm inside builds; on a normal machine `make up` needs none.

## Try it without Docker (dev stack + UI)

```bash
make venv
make dev                          # everything in ONE process on :8000: wallet, bid, capacity, market,
                                  # settlement, simulated GPU nodes and demand bots (fresh state each run)
cd frontend && npm install && npm run dev      # UI on :5173, proxies /api and /auth to the gateway on :8000
# or: npm run build  -> devstack serves frontend/dist at http://localhost:8000
```

The dev stack swaps Kafka for an in-process queue, Celery for asyncio loops, and Postgres for per-service
sqlite files (WAL, writers serialized with `BEGIN IMMEDIATE`). Same service code, same outbox/idempotency logic;
it is **not** a substitute for the compose run, where Postgres row locks and Kafka redelivery are exercised.
With `docker compose up` the UI is served by nginx at http://localhost:8080 (`web` service). Vite dev against compose needs no switch: it proxies to the gateway on :8000 in both modes.

Tested here: unit/property/concurrency tests plus a seeded chaos soak (161), the UI end to end in a headless browser,
and the full compose stack against real Postgres/Kafka/Redis/Celery containers.

## Chaos tests and benchmarks

`tests/harness.py` runs the whole money loop with injected wallet failures (lost requests *and* lost responses),
duplicated/replayed/reordered events, node failures and random client actions, then checks conservation of money,
charge-for-charge equality between market and wallet, and state agreement. It found and fixed two real bugs
(stranded PENDING bids; cancel racing in-flight charges). Numbers and caveats: [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

```
pytest tests/                       # 6 seeds x 80 rounds + regressions, ~25 s
python tools/bench.py soak 12 100   # bigger run with fault counts
python tools/bench.py clearing|tick|submit
```

Cancelling a bid is a local transaction; the refund is issued by settlement after the market's last charge for it
(up to one tick later, longer if the ticker is down).

## Preemption notice

An outbid running job is not killed on the spot. It gets `allocation.warned` and keeps its GPUs, unbilled, for
`GRACE_SECONDS` (default 30; 10 in the dev stack; 0 = old instant preemption), then `allocation.preempted`
(`reason: grace_expired`). Its GPUs stay reserved meanwhile, so the auction clears against the remaining capacity and
the winner waits instead of double-booking. If capacity itself drops (node failure) there is no notice: warned jobs
are dropped first, then losers die at once. A job on notice can **raise its max bid** to stay (below).

## Ticker leader election

Run as many market replicas as you like with `RUN_TICKER=1`. They compete for a lease row in the market database
(`leases`, TTL `LEASE_SECONDS`, default 15). The winner ticks; the others stand by and take over under a higher
**epoch** if it stops renewing. Every tick is stamped with its epoch and written only if the writer still holds
`(holder, epoch)`, checked with a row lock in the same transaction, so a paused ex-leader cannot write a stale tick
(`GET /v1/admin/leader` shows who leads). Covered by tests: standby, expiry takeover, fencing of a deposed leader,
4 threads racing for 12 steps (exactly one writer, no duplicate ticks). Not yet run: two real replicas on Postgres
(the lock is `SELECT ... FOR UPDATE`, a no-op on sqlite where `BEGIN IMMEDIATE` serializes instead).

## Accounts and the API gateway

The browser talks to one service, the gateway (`:8000`). It owns sign-up/login (scrypt password hashes, 1h HS256 JWTs),
verifies the token on every call and applies a default-deny policy before proxying to the internal services; the
backend services no longer publish ports in compose.

- Users see and move only their own wallet, holds, bids and jobs. A bid's `user_id` always comes from the token
  (a mismatching `user_id` is a 403), and cancelling or reading someone else's bid is a 403.
- Market data (prices, ticks, book, reserves, the live WebSocket) is public.
- Fleet, settlement, integrity and `/v1/admin/*` are admin-only. Sign up as **`admin`** (`ADMIN_USERNAMES`) to get
  the "Fleet and checks" tab.
- Internal wallet endpoints (`/v1/holds/*`) are not reachable through the gateway at all.
- 5 failed logins per username per minute return 429, and a user may place 30 bids per 10 s. The counters live in Redis, so
  every gateway replica shares them (without `REDIS_URL` they are per process; if Redis dies they fall back to local counts).
- `JWT_SECRET` must be set to 32+ random characters outside the demo; `ALLOW_TEST_DEPOSIT=0` removes the free-money button.

Not done: refresh tokens/revocation, email verification, per-user rate limits on bidding, service-to-service auth
(services still trust anything that can reach them on the private network), HTTPS termination.

## Database migrations (Alembic, one history per service)

Each service owns `services/<name>/migrations/` and its own database. Compose sets `MIGRATE_ON_START=1`, so every
service runs `upgrade head` at boot (a Postgres advisory lock stops replicas migrating concurrently); sqlite dev and
tests still use `create_all`.

```bash
make migration svc=market m="add column"   # autogenerate the next revision from the models (review it!)
make check-migrations                      # CI guard, also part of pytest
python tools/migrate.py wallet             # apply by hand (needs WALLET_DATABASE_URL)
```

The guard fails if a model changes without a migration, or if a downgrade leaves tables behind. Autogenerate does not
see everything (renames, data backfills, some constraint changes), so read each revision before committing it.
The initial revisions are the schema as of this commit: a Postgres volume created by an earlier `docker compose up`
should be wiped (`docker compose down -v`). Migrations are tested on sqlite only; the Postgres path (advisory lock,
real types) has not been run.

## Scaling the gateway and the live feed

Run any number of gateway replicas behind a load balancer. With `REDIS_URL` set:
the market's leader publishes every committed tick to Redis pub/sub (best effort, after the commit); each gateway
holds **one** subscription and fans out to its own WebSocket clients, instead of every browser tab polling the market.
A slow poll of `/v1/markets/latest` stays on as a safety net and de-duplicates by tick number, so a lost publish or a
Redis outage only delays a tick, never loses one. A client that can't keep up drops its own oldest messages without
stalling the others. Without Redis (the dev stack) the gateway polls the market itself, once per process.

Tested with fakeredis (shared limits across two gateways, dedupe, slow client, publish-after-commit, Redis down).
Not run against a real Redis, and not load-tested: I haven't measured how many clients one gateway sustains.

## Raising a bid (and rescuing a job on notice)

`PATCH /v1/bids/{id}` `{"max_price_micro": ...}` (UI: "Raise bid" on any live job). Prices only go up.
1. The bid service sizes the new escrow for the new price and asks the wallet to **increase the hold** (idempotent on
   the exact change, so a retry never escrows twice; `402` if the balance can't cover it, bid left untouched).
2. The bid row (guarded by a version number, so a concurrent edit gets a `409`) and a `bid.repriced` event commit together.
3. The market applies it by `price_version`: stale or duplicate raises are ignored, and a raise that overtakes the
   original submit (different topics, no ordering) still creates the bid once.
4. If the job was on notice, the raise clears it and the job competes again at the new price, billed from then on.

Anti-abuse: a bid gets **one** notice, ever. A rescued job that is outbid again, or whose raise still isn't enough,
is preempted at once, so nudging the price by a hair can't buy unbilled GPU time. Escrow only grows; unused escrow is
refunded when the job ends. A crash between the wallet increase and the bid commit leaves extra escrow that a retry
re-uses (same key) or the final release refunds.

Also fixed on the way: timestamps sent to the browser are now always zone-aware (sqlite returned naive ones, which
browsers read as local time and showed notice countdowns skewed by the viewer's UTC offset).
