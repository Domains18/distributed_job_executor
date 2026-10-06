# Kombucha — Distributed Job Execution Engine

**Kombucha** is a high-performance, crash-resilient, zero-third-party-dependency distributed job execution engine written in pure Go (standard library only).

It accepts background jobs from web applications, persists them to an append-only segmented Write-Ahead Log (WAL), schedules execution using in-memory priority heaps with queue isolation, and dispatches leases to worker processes over an HTTP long-polling wire protocol with monotonic fencing tokens.

---

## Architecture

```
            HTTP submit                     HTTP long-poll lease
  Client  ─────────────►  ┌───────────────┐  ◄─────────────────  Worker 1
                          │  COORDINATOR  │  ◄─────────────────  Worker 2
  Client  ─────────────►  │               │  ◄─────────────────  Worker 3
                          └───────┬───────┘
                                  │
        ┌─────────────────────────┼─────────────────────────┐
        │                         │                         │
   ┌────▼─────┐            ┌──────▼──────┐           ┌──────▼──────┐
   │ api      │            │  engine     │           │  scheduler  │
   │ net/http │───cmds────►│ single      │◄──timers──│ due + lease │
   │ handlers │◄──results──│ writer loop │           │ expiry      │
   └──────────┘            └──┬───────┬──┘           └─────────────┘
                              │       │
                    ┌─────────▼──┐ ┌──▼──────────┐
                    │  wal       │ │  index      │
                    │ segments   │ │ in-memory   │
                    │ + fsync    │ │ + heaps     │
                    └────────────┘ └─────────────┘
                              │
                      ┌───────▼────────┐
                      │   snapshot     │
                      └────────────────┘
```

---

## Core Principles & Guarantees

- **Zero External Dependencies:** 100% Go standard library (`crypto/rand`, `encoding/binary`, `hash/crc32`, `container/heap`, `net/http`, `log/slog`, `sync`, `time`). No Postgres, Redis, Prometheus, or third-party packages.
- **Single-Writer Durability Invariant:** All state mutations are funneled through a single goroutine loop. Writes and fsync calls are amortized across batches; the in-memory index is updated **only after** `log.Append()` durably returns. Readers access the index concurrently via an `RWMutex`.
- **Fencing Tokens:** Leases issue a monotonic token. Completions or heartbeats presenting a stale token are rejected with `409 Conflict`, neutralizing GC pauses and network delay split-brains.
- **Poison-Pill Mitigation:** The `Attempt` count increments at **lease time**, not failure time. A job that crashes or kills workers will burn an attempt and transition to `StateDead` upon reaching `MaxAttempts`, rather than looping infinitely.
- **Segmented WAL with Castagnoli CRC32C:**
  - Segment files named by base LSN (`00000000000000000001.wal`).
  - Hardware-accelerated CRC32C verification on all disk frames.
  - Automatic torn-tail truncation repair on crashes; sealed segment corruption fails loudly.
  - Sticky error semantics refusing further writes after disk or `fsync` failures.
- **Queue Isolation:** Per-queue priority heaps ordered by `(RunAt, -Priority, ID)`. Floods of low-value `bulk` jobs never walk past or starve jobs in `critical`.
- **Long-Poll Parking Lot:** Single-waiter wakeup per eligible job prevents thundering herd stampedes.
- **Exponential Backoff & Full Jitter:** Retries back off exponentially with full jitter over the top half: `base/2 + rand(base/2)`.
- **Graceful Draining:** Workers intercept `SIGTERM` / `SIGINT`, stop leasing, and await in-flight task completions before shutting down.

---

## Quick Start with Docker

Kombucha includes a multi-stage Docker build and a ready-to-run Docker Compose configuration:

### 1. Start Coordinator and 3 Worker Replicas

```bash
docker compose up --build -d
```

Check cluster status:
```bash
docker compose ps
docker compose logs -f
```

### 2. Run the Demo Script

A demo script is provided to submit sample jobs, inspect progress, and view queue statistics:

```bash
./scripts/demo.sh
```

### 3. Stop Cluster

```bash
docker compose down
```

---

## Running Locally from Source

### Prerequisites

- Go 1.22+ (tested on Go 1.26.2)

### 1. Start the Coordinator

```bash
go run ./cmd/coordinator -data-dir=./data -addr=:8080
```

Flags:
- `-data-dir`: Directory for WAL segments and snapshots (default `./data`).
- `-addr`: HTTP listen address (default `:8080`).
- `-auth-token`: Optional shared bearer authentication token.

### 2. Start Worker Processes

In separate terminals:

```bash
go run ./cmd/worker -coordinator=http://localhost:8080 -id=worker-1 -capacity=10
go run ./cmd/worker -coordinator=http://localhost:8080 -id=worker-2 -capacity=10
```

Flags:
- `-coordinator`: Coordinator HTTP endpoint (default `http://localhost:8080`).
- `-id`: Worker identifier.
- `-queues`: Comma-separated queue names to drain (default `critical,email,default,bulk`).
- `-capacity`: Max concurrent in-flight jobs (default `10`).
- `-auth-token`: Bearer authentication token.

---

## REST API Reference

All requests and responses use JSON. When an auth token is configured, include `Authorization: Bearer <token>`. Unknown request fields trigger `400 Bad Request`.

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/v1/jobs` | Submit a background job (supports delayed `run_at` and idempotency keys) |
| `GET` | `/v1/jobs/{id}` | Query job state, attempts, timestamps, and error message |
| `POST` | `/v1/jobs/{id}/cancel` | Cancel job (immediate if pending, cooperative if leased) |
| `POST` | `/v1/lease` | Long-poll lease up to N jobs across queues with lease duration |
| `POST` | `/v1/jobs/{id}/heartbeat`| Extend lease duration and receive cooperative cancellation notices |
| `POST` | `/v1/jobs/{id}/complete` | Report job success or failure with the fencing token |
| `GET` | `/v1/stats` | Per-queue depth by state, lease counts, oldest pending age |
| `GET` | `/metrics` | Prometheus text format metrics |
| `GET` | `/healthz` | Health check endpoint |

### Examples

#### Submit a Job
```bash
curl -X POST http://localhost:8080/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "order.confirmation_email",
    "queue": "email",
    "payload": {"order_id": "ord-1001", "email": "alice@example.com"},
    "priority": 5,
    "max_attempts": 3,
    "idempotency_key": "email-ord-1001"
  }'
```
Response (`201 Created` or `200 OK` on duplicate submission):
```json
{
  "id": "01M48AEACBN3WNXMCYFGBXSYHG",
  "state": "pending"
}
```

#### Query Job Status
```bash
curl http://localhost:8080/v1/jobs/01M48AEACBN3WNXMCYFGBXSYHG
```

#### Query Queue Statistics
```bash
curl http://localhost:8080/v1/stats
```
Response:
```json
{
  "critical": { "pending": 0, "leased": 1, "succeeded": 142, "dead": 0, "cancelled": 0 },
  "email": { "pending": 2, "leased": 0, "succeeded": 98, "dead": 1, "cancelled": 0, "oldest_pending_age": 4200000000 }
}
```

---

## Testing & Benchmarks

The engine features unit tests, crash property tests, fuzzing, and end-to-end chaos verification:

### Run Full Test Suite with Race Detector
```bash
go test -race ./...
```

### WAL Batched Append Benchmark
```bash
go test -bench=BenchmarkWAL_BatchedAppend -benchmem ./storage/wal/...
```
*Result:* **~690,000 records/second** with zero allocations per frame on reuse.

### Chaos Test (Fault Injection & Consistency)
```bash
go test -v -race -run TestKombuchaCo_ChaosRun ./workers/jobs/...
```
Dispatches 300 jobs across 3 concurrent workers under 10% simulated service failure rates, verifying:
1. Every job reaches a terminal state (`succeeded` or `dead`).
2. Exact inventory balance with **zero double-decrements** via fencing token tracking.
3. Emails sent at most once per order.

---

## Repository Structure

```
├── api/                  # HTTP REST server, bearer auth, and long-poll parking lot
├── cmd/
│   ├── coordinator/      # Coordinator server daemon entry point
│   └── worker/           # Worker runner daemon entry point
├── core/                 # Sortable JobID (Crockford Base32), Job models, records, clocks
├── docs/                 # System architecture specifications and notes
├── scheduler/            # Background timer loop for due promotion and lease expiration
├── scripts/              # Helper scripts (demo, curl examples)
├── storage/
│   ├── engine/           # Single-writer loop, validation, and durability invariants
│   ├── index/            # In-memory index with due-heaps and lease-heaps
│   ├── snapshot/         # Atomic snapshotting with file and directory fsync
│   └── wal/              # Segmented write-ahead log with Castagnoli CRC32C
├── worker/               # Worker client library, lease loop, heartbeats, and graceful drain
├── workers/jobs/         # Kombucha Co application handlers (emails, inventory, payments, etc.)
├── Dockerfile            # Multi-stage build for coordinator and worker
└── docker-compose.yaml   # Clustered deployment configuration
```
