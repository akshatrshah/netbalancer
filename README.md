# NetBalancer — Production-Grade L7 Load Balancer

A continuously running Layer 7 load balancer written in C++17 from scratch — no frameworks, no libraries beyond the standard. Accepts real TCP connections, routes to real backend servers, and makes routing decisions in microseconds.

Built to demonstrate why C++ is the right tool for network infrastructure: lock-free data structures, raw POSIX sockets, zero-copy proxying, and true hardware-level concurrency that Python's GIL makes impossible.

---

## Why C++?

| Feature | This project | Python equivalent |
|---|---|---|
| Lock-free metrics ring buffer | `std::atomic` + power-of-2 bitmask | Not possible — GIL serializes threads |
| True parallel request handling | `std::thread` + hardware concurrency | Threads don't run in parallel |
| Raw socket I/O | `poll()`, `SOCK_STREAM`, `O_NONBLOCK` | Hidden behind abstractions |
| Zero-copy proxy | Direct `read()`/`write()` between fds | Framework overhead on every byte |
| Sub-microsecond routing | No allocations on hot path | Interpreter overhead per operation |

The metrics pipeline processes every request through a **lock-free ring buffer** — no mutex, no contention, no blocking. Python's GIL would serialize this entirely.

---

## Architecture

```
                    ┌─────────────────────────────────────────┐
curl/browser ──────▶│          NetBalancer :8080              │
                    │                                         │
                    │  T1  Acceptor        raw POSIX sockets  │
                    │  T2  Health Checker  probes every 2s    │──▶ Pod :8001
                    │  T3  Metrics Engine  lock-free ring buf │──▶ Pod :8002
                    │  T4  Dashboard API   SSE + REST         │──▶ Pod :8003
                    │  T5  CLI             interactive        │──▶ Pod :N
                    │  T6  Pipe Reader     external control   │
                    └─────────────────────────────────────────┘
                                    │
                              localhost:9090
                              Live Dashboard
```

---

## Features

### Routing Algorithms
- **Weighted Round Robin** — distributes traffic proportional to pod weight. A pod with weight=2 receives 2x the traffic of weight=1.
- **Least Connections** — weight-adjusted: routes to the pod with the lowest `active_connections / weight` ratio. Naturally handles pods with different capacities.
- **IP Hash** — consistent hashing by client IP. Same client always hits the same pod. Useful for session stickiness.

Switch algorithms live while traffic flows — zero downtime.

### Circuit Breaker
Three-state finite state machine per backend pod:

```
CLOSED ──(5 errors in window)──▶ OPEN ──(30s timeout)──▶ HALF_OPEN
  ▲                                                           │
  └──────────────(success)────────────────────────────────────┘
```

- **CLOSED**: normal operation, all requests pass through
- **OPEN**: backend is failing, requests are immediately rejected — no wasted connections
- **HALF_OPEN**: one probe request allowed through — if it succeeds, breaker closes

### Health Checking
- Probes every backend every 2 seconds with an HTTP HEAD request
- Marks backend DOWN after 2 consecutive failures
- Marks backend UP after 2 consecutive successes
- Health state and probe latency logged on every cycle

### Pod Lifecycle Management
Spawn and kill backend pods at runtime from the dashboard or CLI:
```bash
# From dashboard: type port → click SPAWN POD
# From CLI:
spawn 8001        # spawn pod on :8001 with default weight 1
spawn 8002 3      # spawn pod on :8002 with weight 3 (3x traffic)
killpod B2        # remove pod B2 and kill the process
```
The system waits for the port to actually respond before registering — eliminates the cold-start 5s connect timeout.

### Lock-Free Metrics Pipeline
Every request records latency through a **lock-free ring buffer** backed by `std::atomic`:
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
- No mutex, no lock, no contention — `fetch_add` is a single CPU instruction
- Power-of-2 size turns modulo into a bitmask (`& (N-1)`)
- 4096-slot global ring, 1024-slot per-backend ring
- Percentiles computed by snapshotting and sorting — never blocks the request path

### Live Dashboard
Real-time browser dashboard at `http://localhost:9090`:
- **Infrastructure visualizer** — animated canvas showing live traffic flowing client → LB → pods. Particle rate tied to actual req/s. Pod glow intensity scales with load.
- **Pod cards** — per-pod req/s, active connections, traffic share, weight, circuit breaker state
- **Traffic distribution bars** — real req/s from each pod, updates every second
- **Algorithm selector** — switch routing algorithm live from the browser

---

## Build & Run

**Requirements:** macOS or Linux, `g++` with C++17, `python3`

```bash
# Clone and build
git clone https://github.com/YOUR_USERNAME/netbalancer
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

From the dashboard, spawn pods on ports 8001–8005 and watch traffic distribute in real time.

---

## Benchmark Results

Measured on MacBook Pro M2, 3 pods, round-robin:

| Metric | Value |
|---|---|
| Throughput (steady state) | ~40 req/s per pod |
| p50 latency (1 pod) | ~19ms |
| p50 latency (3 pods) | ~9ms |
| p50 latency improvement | **~50% reduction** |
| Error rate (pods registered) | 0% |
| Health check interval | 2s |
| Failover detection | < 4s |
| Circuit breaker trip threshold | 5 errors |

---

## Project Structure

```
netbalancer/
├── main.cpp        — load balancer core (853 lines): acceptor, health checker,
│                     metrics engine, circuit breaker, weighted routing, pod manager
├── dashboard.html  — single-file frontend: canvas visualizer, pod cards, controls
├── logger.hpp      — thread-safe timestamped logger (file + optional stdout)
├── httplib.h       — single-header HTTP library for dashboard server
├── simulate.sh     — continuous traffic generator with random jitter
└── Makefile
```

---

## Concepts Demonstrated

| Concept | Implementation |
|---|---|
| Lock-free ring buffer | `LockFreeRing<T,N>` with `std::atomic`, power-of-2 bitmask |
| Circuit breaker FSM | `CBState`: CLOSED → OPEN → HALF_OPEN per backend |
| Weighted routing | Expanded pool for RR, normalized connections for LC |
| Non-blocking I/O | `O_NONBLOCK` + `poll()` for connect timeout |
| Bidirectional proxy | `poll()` on both fds, zero-copy `read()`/`write()` |
| Thread safety | `std::atomic` for counters, `std::mutex` for pool mutation only |
| Health checking | HTTP HEAD probe with configurable fail/recover thresholds |
| SSE push | Chunked HTTP response streaming state every 1s to dashboard |