# Kombucha — Distributed Job Execution Engine

A zero-dependency distributed job execution engine in Go that accepts background tasks and reliably executes them across a cluster of workers with lease-based dispatch, fencing tokens, exponential backoff, and crash-resilient write-ahead logging.

## Architecture

```
            HTTP submit                     HTTP long-poll lease
  Client  ─────────────►  ┌───────────────┐  ◄─────────────────  Worker A
                          │  COORDINATOR  │  ◄─────────────────  Worker B
  Client  ─────────────►  │               │  ◄─────────────────  Worker C
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

- **Storage:** Segmented Write-Ahead Log (WAL) with Castagnoli CRC32C, periodic atomic snapshots, and in-memory index with due/lease heaps. Zero external database dependencies.
- **Single-Writer Loop:** Funnels all state mutations sequentially, amortizing fsync latency across batches and guaranteeing strict ordering.
- **Workers:** Pull work via HTTP long-poll. Leases use monotonic fencing tokens (`409 Conflict` on stale token completion) to prevent GC-pause split brain.
