# Benchmarks and chaos results

Reproduce everything: `python tools/bench.py clearing|tick|submit|soak [seeds rounds]` and `pytest tests/`.
Raw output of the runs below is in `docs/bench/*.json`.

**Read this first.** Sections 1-2 run in one process on one machine (4-core aarch64 Linux VM, Python 3.10) with
sqlite files. They measure *our* code (the auction, the DB access pattern, request handling) and above all
**correctness under failure**, not Postgres, Kafka or a network. Their throughput numbers are a floor set by sqlite's
single writer. Section 3 is a separate run on real Postgres, Redis, Kafka and Celery, and its numbers are the ones to quote.

## 1. Chaos / soak: money and state survive failures

`tests/harness.py` runs the whole loop (bid API, wallet, market, settlement, event bus) with a seeded RNG, so
any failure replays exactly from its seed. It injects, at random:

| Fault | How |
|---|---|
| Lost request to the wallet | `ConnectError` before the call executes (bid service and settlement) |
| **Lost response** from the wallet | the call executes, the caller sees a timeout (the dangerous one for money) |
| Duplicate and replayed events | at-least-once delivery: ~12% duplicated, ~5% replayed from history |
| Reordering across topics | per-topic order kept, no order between topics (like Kafka) |
| Node failures | per-GPU capacity randomly drops to 0 / half and recovers |
| Client behaviour | random submits (retried with the same request id), cancels, raises of live bids |

After a wind-down it asserts: wallet ledger integrity; **conservation** (users' balances + captured revenue =
total deposited); every escrow closed and no money held; for **every** bid the market's charges equal what the wallet
captured (cancelled or not); no DEAD or skipped settlement work; reconcile clean; bid-service and market state agree;
no bid still live anywhere; GPUs never overbooked after any tick.

12 seeds x 100 rounds (`docs/bench/soak.json`), 30 s wall:

| | |
|---|---|
| Bids / ticks cleared | 814 / 3,636 |
| Wallet requests lost / responses lost | 801 / 769 |
| Events duplicated / replayed | 704 / 293 |
| Capacity changes (node failures) | 306 |
| Settlement retries | 1,289 |
| Warnings (preemption notices) / preemptions / completions | 182 / 300 / 70 |
| Raises accepted / cancels accepted | 186 / 147 |
| **Invariant violations** | **0** |

The harness checks itself: a deliberately broken settlement (fresh idempotency key on every retry) is caught on
5 of 5 seeds.

### Real bugs this found (both fixed, both have regression tests)

1. **A bid could be stranded in PENDING.** If the wallet answered a hold with a lost response, the bid sat PENDING
   with money escrowed. A client retry with the same `client_request_id` returned the PENDING row without finishing
   it, and no sweeper existed (a code comment claimed one did). Now a retry resumes the activation, a sweeper
   finishes the rest, and a conditional UPDATE makes retry-vs-sweeper harmless (exactly one emits the event).
2. **Cancelling could give away compute.** Cancel released the escrow itself, racing with charges still in flight:
   a tick the job really ran was later "skipped" because the hold was already closed (about 13% of charged revenue
   in the first soak), and a lost wallet response left a cancelled-looking job running unbilled. Now cancel is one
   local transaction (status + `bid.cancelled`) that never calls the wallet. The market announces the cancelled
   bid in its next `market.cleared`, after every charge it made for the bid, and settlement's existing
   "RELEASE waits for earlier items" rule refunds behind them. Trade-off: the refund arrives up to one tick
   (5 s) after the click, and if the ticker is down the escrow waits (locked, never lost).

## 2. Component benchmarks (sqlite, single process)

**Clearing engine** (`clear()`, pure function; 30% divisible bids, 10% already running; median of 5):

| Bids | Median |
|---|---|
| 100 | 0.13 ms |
| 1,000 | 0.87 ms |
| 10,000 | 6.0 ms |
| 50,000 | 26 ms |

Roughly linear. A 5-second tick has ~190x headroom even at 50k bids per market.

**Full market tick** (`run_tick` incl. DB reads/writes, outbox events, in-memory sqlite, 256 GPUs; median of 5):

| Queued bids | First tick | Steady state |
|---|---|---|
| 100 | 5.7 ms | 2.0 ms |
| 1,000 | 10 ms | 7.7 ms |
| 5,000 | 44 ms | 40 ms |

**HTTP, bid service + wallet in the loop, in-process dev stack** (ASGI, file sqlite, escrow placed per request).
These are NOT the real-stack numbers; see section 3 (about 46/s on Postgres + Kafka):

| Endpoint | Concurrency | Throughput | p50 | p95 | p99 |
|---|---|---|---|---|---|
| `POST /v1/bids` | 1 | 156/s | 5 ms | 11 ms | 17 ms |
| `POST /v1/bids` | 8 | 196/s | 13 ms | 141 ms | 507 ms |
| `POST /v1/bids` | 32 | 192/s | 55 ms | 798 ms | 1.6 s |
| `GET /v1/bids?user_id=` | 32 | 690/s | 18 ms | 72 ms | 644 ms |

Throughput flattens at about 190 bids/s and tail latency grows with concurrency: each submit does three commits
across two sqlite files, and sqlite has one writer. That is the dev-stack ceiling. With Postgres the per-service
databases remove that bottleneck, and the service scales horizontally behind the gateway (stateless, shared Redis
rate limits), but I have not measured it, so I make no claim about the number.

## 3. Real infrastructure run (Postgres 16, Redis 7, Kafka 3.8, real Celery)

Two runs on a 2-CPU sandbox that also ran the load generator: (a) the services as separate processes with the
`docker-compose.yml` environment against real Postgres/Redis/Kafka containers (functional checks + the throughput
table below), and (b) the **complete compose stack as containers**, with every Dockerfile built from the repo
(8 backend images + the nginx UI image): the same 60 s job gave identical money results (12 captures = 26,676,
refund 13,332, integrity and reconcile `ok`, no tracebacks in any container) and the UI/nginx proxy answered.
The sandbox needed a CA-trust workaround for pip/npm during builds; that is an environment limit, not a project issue.

| Check | Result |
|---|---|
| Alembic migrations on Postgres, all 6 services | applied on boot, no errors |
| Bid via gateway to RUNNING (Kafka path) | 2 s |
| 60 s job to COMPLETED | 12 Celery captures = 26,676 and refund 13,332; wallet ended at exactly 50,000,000 - 26,676 |
| Wallet integrity / settlement reconcile | both `ok: true` |
| Cancel a running bid | refund landed 3.1 s later (market announces, settlement releases), integrity clean |
| Service logs | no tracebacks |

Bid submit throughput, bid service on Postgres, wallet over HTTP, outbox relaying to Kafka:

| Concurrency | Throughput | p50 | p95 | p99 |
|---|---|---|---|---|
| 1 | 30/s | 32 ms | 54 ms | 80 ms |
| 8 | 48/s | 162 ms | 255 ms | 322 ms |
| 32 | 46/s | 613 ms | 1.2 s | 1.8 s |

At concurrency 32, 3 of 1,500 requests returned 503 (wallet call timed out). The honest end-to-end figure on this
stack is **about 45 bids/s**, well below the 190/s of the in-process dev stack: real fsync commits, an HTTP hop to
the wallet per bid, and 2 shared CPUs. A multi-node deployment would likely do better; that is unmeasured.

## 4. What is not measured

- Throughput of the containerised stack (the table above is from the process run).
- The chaos soak against the real stack (fault injection is in-process; the real run was a functional check).
- Leader failover time is bounded by construction (`LEASE_SECONDS` + one tick) and covered by `test_leader.py`
  (fencing, stale leaders, takeover); no wall-clock failover number is claimed.
- The gateway's auth overhead and WebSocket fan-out under load.
