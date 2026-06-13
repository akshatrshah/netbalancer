# NetBalancer — Production L7 Load Balancer

A plug-and-play Layer 7 load balancer written in C++17 from scratch. Point it at any HTTP backend — Node, Go, Java, Python, anything — and it handles routing, health checking, circuit breaking, and rate limiting automatically.

No frameworks. No external networking libraries. Drop it in front of any service.

---

## Quick Start

```bash
git clone https://github.com/akshatrshah/netbalancer
cd netbalancer
curl -o httplib.h https://raw.githubusercontent.com/yhirose/cpp-httplib/master/httplib.h
make

# Edit config.json with your backends
./netbalancer --config config.json

# Dashboard
open http://localhost:9090

# Prometheus metrics
curl http://localhost:9090/metrics
```

---

## Configuration

All settings configurable via `config.json` or CLI flags. CLI flags take precedence.

**config.json**
```json
{
  "port": 8080,
  "dashboard_port": 9090,
  "algorithm": "rr",
  "health_path": "/health",
  "backends": [
    {"host": "10.0.1.5",  "port": 3000, "weight": 2},
    {"host": "10.0.1.6",  "port": 3000, "weight": 1},
    {"host": "api.myservice.com", "port": 8080, "weight": 1}
  ]
}
```

**CLI flags**
```bash
./netbalancer --port 80 --config /etc/netbalancer/prod.json
./netbalancer --port 443 --algo lc --health /ping --dashboard 9090
./netbalancer --help
```

| Flag | Default | Description |
|---|---|---|
| `--port` | 8080 | Load balancer listen port |
| `--dashboard` | 9090 | Dashboard + Prometheus port |
| `--config` | config.json | Config file path |
| `--algo` | rr | Routing algorithm (rr/lc/hash) |
| `--health` | /health | Health check endpoint |

---

## Why C++?

| Feature | This project | Python equivalent |
|---|---|---|
| Lock-free ring buffer | `std::atomic` + power-of-2 bitmask | Not possible — GIL serializes threads |
| True parallel threads | 5 concurrent threads, real parallelism | GIL prevents true parallelism |
| kqueue event loop | Single-threaded I/O, kernel notifications | Hidden behind async frameworks |
| Raw socket I/O | `poll()`, `SOCK_STREAM`, `O_NONBLOCK` | Framework overhead on every call |
| Token bucket rate limiter | Per-IP accounting, zero contention | GIL contention at high req/s |

---

## Architecture

```
                    ┌──────────────────────────────────────────┐
Any HTTP client ───▶│         NetBalancer :8080                │
(curl, browser,     │                                          │
 your service)      │  T1  kqueue event loop   all TCP I/O     │──▶ Backend A :3000
                    │  T2  Health Checker      probes every 2s │──▶ Backend B :3000
                    │  T3  Metrics Engine      lock-free ring  │──▶ Backend N :XXXX
                    │  T4  Dashboard + API     SSE + REST      │
                    │  T5  Pipe Reader         external ctrl   │
                    └──────────────────────────────────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │  localhost:9090               │
                    │  Dashboard  │  /metrics       │
                    └───────────────────────────────┘
```

### I/O Model — kqueue Event Loop (macOS)

All TCP I/O runs on a single thread via kernel event notifications.
No per-connection threads, no blocking, no spinning.

```
Traditional (per-thread):     kqueue (this project):
  100 connections              100 connections
  = 100 threads                = 1 I/O thread
  = 100 × 8MB stack            = kernel notifies on readiness
  = context switch overhead    = zero idle CPU cost
```

Linux fallback uses `poll()` + per-connection threads.

---

## Features

### Routing Algorithms
- **Weighted Round Robin** — distributes traffic proportional to weight. `rr_index` resets on pool changes to prevent skew. Update weights live without restart.
- **Least Connections** — weight-adjusted: routes to backend with lowest `active_connections / weight`. Handles heterogeneous capacity.
- **IP Hash** — consistent hashing by client IP. Same client always hits same backend. Useful for session stickiness.

Switch algorithms live — zero downtime, zero dropped connections.

### Circuit Breaker
Three-state FSM per backend:

```
CLOSED ──(5 errors)──▶ OPEN ──(30s timeout)──▶ HALF_OPEN
  ▲                                                  │
  └──────────────(1 success)─────────────────────────┘
```

- **CLOSED** — normal, all requests pass through
- **OPEN** — backend failing, requests rejected immediately
- **HALF_OPEN** — one probe allowed, closes on success

### Token Bucket Rate Limiter
Per client IP:
- 100 tokens burst capacity
- Refills at 50 tokens/second
- Requests exceeding bucket → HTTP 429

### Lock-Free Ring Buffer
Every request records latency through a lock-free ring buffer — no mutex on the hot path:

```cpp
void push(T val) {
    size_t slot = head.fetch_add(1, std::memory_order_relaxed) & (N-1);
    buf[slot].store(val, std::memory_order_relaxed);
}
```

- `fetch_add` = single CPU instruction
- Power-of-2 size → bitmask instead of division
- 4096-slot global ring, 1024-slot per-backend ring

### Prometheus Observability
Standard Prometheus text format at `/metrics`:

```
netbalancer_requests_total 3936
netbalancer_latency_p50_ms 10.1
netbalancer_latency_p99_ms 33.8
netbalancer_backend_healthy{backend="B1",host="10.0.1.5:3000"} 1
netbalancer_backend_circuit_breaker{backend="B1",host="10.0.1.5:3000"} 0
netbalancer_backend_requests_total{backend="B1",host="10.0.1.5:3000"} 1312
```

Works with any standard Prometheus scraper. No client libraries needed.

### Zero-Downtime Config Hot Reload
```bash
# Edit config.json, then:
kill -HUP $(pgrep netbalancer)

# Or from CLI:
netbalancer> reload
```

Backends update, algorithm changes, weights adjust — zero dropped connections.

### Live Weight Updates
```bash
# From CLI
netbalancer> weight B1 3
netbalancer> weight B2 1

# From dashboard — BACKENDS tab → UPDATE WEIGHT
```

### Health Checking
- Configurable endpoint (default `/health`, set via `--health` flag)
- Probes every 2 seconds per backend
- Marks DOWN after 2 consecutive failures
- Marks UP after 2 consecutive successes

### Live Dashboard
Real-time browser dashboard at `http://localhost:9090`:

- **TRAFFIC tab** — ingress address, live req/s per backend, rate limiter stats
- **BACKENDS tab** — add/remove any backend (any host, any port), update weights, dev test tools clearly separated
- **REQUEST LOG tab** — last 30 requests with backend, path, latency in real time
- **Visualizer** — animated canvas showing live traffic flow, particle rate tied to real req/s, circuit breaker state visible per backend

---

## CLI Commands

```
add <host> <port> [weight]   Add backend server (any host/IP)
remove <id>                  Remove backend by ID
weight <id> <n>              Update backend weight live
algo <rr|lc|hash>            Switch routing algorithm
reload                       Hot reload config.json
status                       Live status
bench                        Metrics summary
benchmark [N] [C]            Run N requests at C concurrency
spawn <port> [weight]        Start local test server (dev only)
killpod <id>                 Stop local test server (dev only)
help                         Show all commands
exit                         Shutdown
```

---

## Benchmark Results

Measured on MacBook Pro, 5 backends, round-robin, kqueue event loop:

| Concurrency | Requests | Throughput | p50 | p99 | Errors |
|---|---|---|---|---|---|
| 100 | 10,000 | 580 req/s | 88ms | 190ms | 0% |
| 200 | 10,000 | 551 req/s | 132ms | 360ms | 0% |
| 1,000 | 10,000 | 1,629 req/s | 216ms | 468ms | 60%* |

*High-concurrency errors are backend saturation, not load balancer failure.

Steady state (3 backends, continuous load): **p50 10ms, p99 34ms, 0% errors**

---

## Project Structure

```
netbalancer/
├── main.cpp        — core: kqueue event loop, health checker, metrics engine,
│                     circuit breaker, weighted routing, rate limiter,
│                     Prometheus endpoint, SIGHUP hot reload, CLI arg parsing
├── dashboard.html  — single-file frontend: canvas visualizer, request log,
│                     backend management, algorithm selector
├── config.json     — hot-reloadable configuration
├── logger.hpp      — thread-safe timestamped logger
├── httplib.h       — single-header HTTP library (dashboard only)
├── simulate.sh     — continuous traffic generator for testing
└── Makefile        — auto-detects macOS for kqueue, falls back to poll on Linux
```

---

## Concepts Demonstrated

| Concept | Implementation |
|---|---|
| kqueue event loop | Edge-triggered, single-threaded I/O |
| Lock-free ring buffer | `std::atomic`, power-of-2 bitmask, no mutex on hot path |
| Circuit breaker FSM | CLOSED → OPEN → HALF_OPEN per backend |
| Token bucket rate limiter | Per-IP burst + sustained rate |
| Weighted routing | Expanded pool for RR, normalized connections for LC |
| Non-blocking I/O | `O_NONBLOCK` + `connect_to_backend_nb()` |
| Health checking | HTTP HEAD probe, configurable endpoint |
| Prometheus metrics | Standard text format, per-backend labeled counters/gauges |
| SIGHUP hot reload | Zero-downtime backend and algorithm reconfiguration |
| CLI configurability | `--port`, `--config`, `--algo`, `--health`, `--dashboard` |
| Request audit log | Fixed ring, real-time dashboard feed |