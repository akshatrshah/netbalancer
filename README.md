# NetBalancer — Production L7 Load Balancer

A continuously running Layer 7 load balancer written in C++17 from scratch. Accepts real TCP connections, routes to real backend servers, and makes routing decisions in microseconds. No frameworks, no external networking libraries.

---

## Why C++?

| Feature | This project | Python equivalent |
|---|---|---|
| Lock-free ring buffer | `std::atomic` + power-of-2 bitmask | Not possible — GIL serializes threads |
| True parallel threads | 5 concurrent threads, real parallelism | GIL prevents true parallelism |
| kqueue event loop | Single-threaded I/O, kernel notifications | Hidden behind async frameworks |
| Raw socket I/O | `poll()`, `SOCK_STREAM`, `O_NONBLOCK` | Framework overhead on every call |
| Token bucket rate limiter | Per-IP accounting with zero contention | GIL contention at high req/s |

---

## Architecture

```
                    ┌──────────────────────────────────────────┐
curl/browser ──────▶│         NetBalancer :8080                │
                    │                                          │
                    │  T1  kqueue event loop   all TCP I/O     │──▶ Pod :8001
                    │  T2  Health Checker      probes every 2s │──▶ Pod :8002
                    │  T3  Metrics Engine      lock-free ring  │──▶ Pod :N
                    │  T4  Dashboard + API     SSE + REST      │
                    │  T5  Pipe Reader         external ctrl   │
                    └──────────────────────────────────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │       localhost:9090           │
                    │   Dashboard  │  /metrics       │
                    └───────────────────────────────┘
```

### I/O Model — kqueue Event Loop

On macOS, all TCP I/O runs on a single thread via kernel event notifications. No per-connection threads, no blocking, no spinning.

```
Traditional (per-thread):     kqueue (this project):
  100 connections              100 connections
  = 100 threads                = 1 I/O thread
  = 100 × 8MB stack            = kernel notifies on readiness
  = context switch overhead    = zero idle CPU cost
```

---

## Features

### Routing Algorithms
- **Weighted Round Robin** — distributes traffic proportional to pod weight. `rr_index` resets on pool changes to prevent skew.
- **Least Connections** — weight-adjusted: routes to pod with lowest `active_connections / weight`. Handles heterogeneous pod capacity.
- **IP Hash** — consistent hashing by client IP. Same client always hits same pod.

Switch algorithms live while traffic flows — zero downtime.

### Circuit Breaker
Three-state FSM per backend pod:

```
CLOSED ──(5 errors)──▶ OPEN ──(30s timeout)──▶ HALF_OPEN
  ▲                                                  │
  └──────────────(1 success)─────────────────────────┘
```

- **CLOSED** — normal, all requests pass through
- **OPEN** — backend failing, requests rejected immediately, no wasted connections
- **HALF_OPEN** — one probe allowed through, closes on success

### Token Bucket Rate Limiter
Per client IP using a token bucket algorithm:
- 100 tokens burst capacity per IP
- Refills at 50 tokens/second
- Requests exceeding bucket return HTTP 429

### Lock-Free Ring Buffer
Every request records latency through a lock-free ring buffer:

```cpp
template<typename T, size_t N>
struct LockFreeRing {
    static_assert((N & (N-1)) == 0, "N must be power of 2");
    std::array<std::atomic<T>, N> buf;
    std::atomic<size_t> head{0};

    void push(T val) {
        size_t slot = head.fetch_add(1, std::memory_order_relaxed) & (N-1);
        buf[slot].store(val, std::memory_order_relaxed);
    }
};
```

- `fetch_add` is a single CPU instruction — no mutex
- Power-of-2 size turns modulo into bitmask (`& (N-1)`)
- 4096-slot global ring, 1024-slot per-pod ring

### Prometheus Observability
Standard Prometheus text format at `/metrics`:

```
netbalancer_requests_total 3936
netbalancer_latency_p50_ms 10.1
netbalancer_latency_p99_ms 33.8
netbalancer_backend_healthy{backend="B1",host="localhost:8001"} 1
netbalancer_backend_circuit_breaker{backend="B1",host="localhost:8001"} 0
```

Scraped by any standard Prometheus server. No client libraries needed.

### Zero-Downtime Config Hot Reload
Edit `config.json`, send SIGHUP — backends update, algorithm changes, zero dropped connections:

```bash
kill -HUP $(pgrep netbalancer)
# or from CLI:
netbalancer> reload
```

```json
{
  "algorithm": "lc",
  "backends": [
    {"host": "localhost", "port": 8001, "weight": 1},
    {"host": "localhost", "port": 8002, "weight": 2}
  ]
}
```

### Health Checking
- HTTP HEAD probe every 2 seconds per backend
- Marks DOWN after 2 consecutive failures
- Marks UP after 2 consecutive successes

### Pod Lifecycle Management
```bash
spawn 8001        # start pod on :8001, weight=1
spawn 8002 3      # start pod on :8002, weight=3
killpod B2        # remove B2 and kill the process
```

### Request Audit Log
Last 30 requests visible in dashboard — timestamp, pod, path, latency, status.

---

## Build & Run

**Requirements:** macOS or Linux, `g++` with C++17, `python3`

```bash
git clone https://github.com/akshatrshah/netbalancer
cd netbalancer
curl -o httplib.h https://raw.githubusercontent.com/yhirose/cpp-httplib/master/httplib.h
make

# Terminal 1
make run

# Terminal 2
./simulate.sh

# Browser
open http://localhost:9090

# Prometheus metrics
curl http://localhost:9090/metrics
```

---

## Benchmark Results

Measured on MacBook Pro, 5 pods, round-robin, kqueue event loop:

| Concurrency | Requests | Throughput | p50 | p99 | Error Rate |
|---|---|---|---|---|---|
| 100 | 10,000 | 580 req/s | 88ms | 190ms | 0% |
| 200 | 10,000 | 551 req/s | 132ms | 360ms | 0% |
| 1,000 | 10,000 | 1,629 req/s | 216ms | 468ms | 60.7%* |

*Errors at 1000 concurrency are Python backends saturating, not the load balancer.

Steady state (simulate.sh, 3 pods): **p50 10ms, p99 34ms, 0% errors**

---

## Project Structure

```
netbalancer/
├── main.cpp        — core: kqueue event loop, health checker, metrics engine,
│                     circuit breaker, weighted routing, rate limiter,
│                     Prometheus endpoint, SIGHUP hot reload, pod manager
├── dashboard.html  — single-file frontend: canvas visualizer, request log,
│                     pod cards, algorithm selector
├── config.json     — hot-reloadable backend configuration
├── logger.hpp      — thread-safe timestamped logger
├── httplib.h       — single-header HTTP library (dashboard server only)
├── simulate.sh     — continuous traffic generator with random jitter
└── Makefile        — auto-detects macOS for kqueue, falls back to poll on Linux
```

---

## Concepts Demonstrated

| Concept | Implementation |
|---|---|
| kqueue event loop | Edge-triggered, single-threaded I/O, all connections |
| Lock-free ring buffer | `std::atomic`, power-of-2 bitmask, no mutex on hot path |
| Circuit breaker FSM | CLOSED → OPEN → HALF_OPEN per backend |
| Token bucket rate limiter | Per-IP burst + sustained rate with token refill |
| Weighted routing | Expanded pool for RR, normalized connections for LC |
| Non-blocking I/O | `O_NONBLOCK` + `connect_to_backend_nb()` |
| Health checking | HTTP HEAD probe, configurable fail/recover thresholds |
| Prometheus metrics | Standard text format, per-backend labeled counters/gauges |
| SIGHUP hot reload | Zero-downtime backend and algorithm reconfiguration |
| Request audit log | Fixed ring, real-time dashboard feed |
| SSE push | Chunked HTTP streaming state every 1s to dashboard |