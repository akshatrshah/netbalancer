# NetBalancer — Production L7 Load Balancer

A continuously running Layer 7 load balancer written in C++17 from scratch — no frameworks, no external networking libraries. Accepts real TCP connections, routes to real backend servers, and makes routing decisions in microseconds.

---

## Why C++?

| Feature | This project | Python equivalent |
|---|---|---|
| Lock-free metrics ring buffer | `std::atomic` + power-of-2 bitmask | Not possible — GIL serializes threads |
| True parallel request handling | `std::thread` + hardware concurrency | Threads don't run in parallel |
| kqueue event loop | Single thread, kernel notifications | Hidden behind async frameworks |
| Raw socket I/O | `poll()`, `SOCK_STREAM`, `O_NONBLOCK` | Framework overhead on every call |
| Token bucket rate limiter | Lock-free per-IP accounting | GIL contention at high req/s |

The metrics pipeline processes every request through a **lock-free ring buffer** — no mutex, no contention on the hot path. Python's GIL makes this impossible.

---

## Architecture

```
                    ┌──────────────────────────────────────────┐
curl/browser ──────▶│         NetBalancer :8080                │
                    │                                          │
                    │  T1  kqueue event loop   all TCP I/O     │──▶ Pod :8001
                    │  T2  Health Checker      probes every 2s │──▶ Pod :8002
                    │  T3  Metrics Engine      lock-free ring  │──▶ Pod :N
                    │  T4  Dashboard API       SSE + REST      │
                    │  T5  Pipe Reader         external ctrl   │
                    └──────────────────────────────────────────┘
                                    │
                              localhost:9090
                              Live Dashboard
```

### I/O Model — kqueue Event Loop

On macOS, a single thread handles all connections via kernel event notifications. No per-connection threads, no blocking, no spinning.

```
Traditional (per-thread):     kqueue (this project):
  50 connections               50 connections
  = 50 threads                 = 1 thread
  = 50 × 8MB stack             = kernel notifies on readiness
  = high tail latency          = sub-ms routing overhead
```

---

## Features

### Routing Algorithms
- **Weighted Round Robin** — distributes traffic proportional to pod weight. A pod with `weight=2` receives 2x the traffic of `weight=1`. `rr_index` resets on pool changes to prevent skew.
- **Least Connections** — weight-adjusted: routes to pod with lowest `active_connections / weight`. Handles heterogeneous pod capacity.
- **IP Hash** — consistent hashing by client IP. Same client always hits same pod. Useful for session stickiness.

Switch algorithms live while traffic flows — zero downtime.

### Circuit Breaker
Three-state FSM per backend pod:

```
CLOSED ──(5 errors)──▶ OPEN ──(30s timeout)──▶ HALF_OPEN
  ▲                                                  │
  └──────────────(1 success)─────────────────────────┘
```

- **CLOSED** — normal, all requests pass through
- **OPEN** — backend failing, requests rejected immediately
- **HALF_OPEN** — one probe allowed, closes on success

### Token Bucket Rate Limiter
Per client IP rate limiting using a token bucket algorithm:
- Each IP gets 100 tokens (burst capacity)
- Tokens refill at 50/second
- Requests exceeding the bucket return HTTP 429
- Zero mutex contention — bucket state is per-IP and isolated

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

### Health Checking
- Probes every backend every 2 seconds via HTTP HEAD
- Marks DOWN after 2 consecutive failures
- Marks UP after 2 consecutive successes
- Probe latency logged on every cycle

### Pod Lifecycle Management
Spawn and kill backend pods at runtime from the dashboard:
```bash
spawn 8001        # start pod on :8001, weight=1
spawn 8002 3      # start pod on :8002, weight=3
killpod B2        # remove B2 and kill the process
```
Waits for port to respond before registering — eliminates cold-start timeouts.

### Request Audit Log
Every completed request is logged with:
- Timestamp, pod ID, client IP, URL path, latency, HTTP status
- Last 30 requests visible in the dashboard log tab in real time
- Useful for auditing routing decisions and debugging distribution

### Live Dashboard
Real-time browser dashboard at `http://localhost:9090`:
- **Traffic flow visualizer** — animated canvas, particles travel client → LB → pod. Particle rate and LB glow intensity tied to real req/s. Pod boxes show CB state and live req/s.
- **Request log tab** — every request routed with pod, path, latency
- **Traffic distribution** — live req/s per pod (not cumulative), updates every second
- **Algorithm selector** — switch routing algorithm live

---

## Build & Run

**Requirements:** macOS or Linux, `g++` with C++17, `python3`

```bash
git clone https://github.com/akshatrshah/netbalancer
cd netbalancer
curl -o httplib.h https://raw.githubusercontent.com/yhirose/cpp-httplib/master/httplib.h
make

# Terminal 1 — start the load balancer
make run

# Terminal 2 — continuous traffic generator  
./simulate.sh

# Browser
open http://localhost:9090
```

Spawn pods from the dashboard: type a port number and click **SPAWN POD**.

---

## Benchmark Results

Measured on MacBook Pro, 3 pods, round-robin:

| Metric | Value |
|---|---|
| Throughput at 50 concurrent | 415 req/s |
| Throughput at 100 concurrent | 356 req/s |
| p50 latency | ~57ms |
| p99 latency | ~160ms |
| p50 improvement (1→3 pods) | 50% reduction |
| Error rate at steady state | 0% |
| Health check failover | < 4 seconds |
| Circuit breaker trip | 5 consecutive errors |
| Rate limit | 100 req/s burst, 50/s sustained per IP |

---

## Project Structure

```
netbalancer/
├── main.cpp        — core: kqueue event loop, health checker, metrics engine,
│                     circuit breaker, weighted routing, rate limiter, pod manager
├── dashboard.html  — single-file frontend: canvas visualizer, request log,
│                     pod cards, algorithm selector
├── logger.hpp      — thread-safe timestamped logger
├── httplib.h       — single-header HTTP library (dashboard server only)
├── simulate.sh     — continuous traffic generator with random jitter
└── Makefile        — auto-detects macOS for kqueue, falls back to poll on Linux
```

---

## Concepts Demonstrated

| Concept | Implementation |
|---|---|
| kqueue event loop | Edge-triggered, single thread, all connections |
| Lock-free ring buffer | `std::atomic`, power-of-2 bitmask, no mutex on hot path |
| Circuit breaker FSM | CLOSED → OPEN → HALF_OPEN per backend |
| Token bucket rate limiter | Per-IP burst + sustained rate with refill |
| Weighted routing | Expanded pool for RR, normalized connections for LC |
| Non-blocking I/O | `O_NONBLOCK` + `poll()` for connect timeout |
| Bidirectional proxy | `poll()` on both fds, zero-copy `read()`/`write()` |
| Health checking | HTTP HEAD probe, configurable fail/recover thresholds |
| Request audit log | Fixed ring buffer, real-time dashboard feed |
| SSE push | Chunked HTTP streaming state every 1s to dashboard |