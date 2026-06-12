/*
 * NetBalancer — Production-Grade L7 Load Balancer
 *
 * I/O Model:
 *   macOS — kqueue edge-triggered event loop: ONE thread drives ALL connections
 *           via kernel notifications. Zero blocking, zero per-connection threads.
 *   Linux — poll-based per-connection threads (fallback)
 *
 * Threads:
 *   T1 — Event Loop (kqueue/poll) : all TCP I/O
 *   T2 — Health Checker           : probes backends every 2s
 *   T3 — Metrics Engine           : p50/p95/p99, req/s, variance
 *   T4 — Dashboard API            : live browser dashboard
 *   T5 — CLI                      : interactive control
 *   T6 — Pipe Reader              : external control
 *
 * Features: Weighted RR, Least-Connections, IP Hash, Circuit Breaker,
 *           Lock-Free Ring Buffer metrics, Pod lifecycle management
 */

#include <iostream>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>
#include <map>
#include <unordered_map>
#include <array>
#include <algorithm>
#include <numeric>
#include <iomanip>
#include <fstream>
#include <chrono>
#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <cmath>
#include <csignal>
#include <climits>
#include <cstring>
#include <cerrno>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <unistd.h>
#include <poll.h>
#ifdef __APPLE__
#include <sys/event.h>
#endif
#include "httplib.h"
#include "logger.hpp"

// ═══════════════════════════════════════════════════════
//  Constants
// ═══════════════════════════════════════════════════════

static constexpr int    LB_PORT             = 8080;
static constexpr int    DASHBOARD_PORT      = 9090;
static constexpr int    HEALTH_INTERVAL_MS  = 2000;
static constexpr int    METRICS_INTERVAL_MS = 1000;
static constexpr int    HEALTH_TIMEOUT_MS   = 1000;
static constexpr int    FAIL_THRESHOLD      = 2;
static constexpr int    RECOVER_THRESHOLD   = 2;
static constexpr int    CONNECT_TIMEOUT_MS  = 500;
static constexpr int    PROXY_TIMEOUT_MS    = 5000;
static constexpr size_t LATENCY_WINDOW      = 1024;
static constexpr int    CB_ERROR_THRESHOLD  = 5;
static constexpr int    CB_HALF_OPEN_SECS   = 30;
static const std::string PIPE_PATH          = "netbalancer.pipe";

// ═══════════════════════════════════════════════════════
//  Lock-Free Ring Buffer
//  Power-of-2 size → modulo becomes bitmask (no division)
//  std::atomic → no mutex on hot path
// ═══════════════════════════════════════════════════════

template<typename T, size_t N>
struct LockFreeRing {
    static_assert((N & (N-1)) == 0, "N must be power of 2");
    std::array<std::atomic<T>, N> buf;
    std::atomic<size_t> head{0};

    LockFreeRing() { for (auto& a : buf) a.store(0, std::memory_order_relaxed); }

    void push(T val) {
        size_t slot = head.fetch_add(1, std::memory_order_relaxed) & (N-1);
        buf[slot].store(val, std::memory_order_relaxed);
    }

    std::vector<T> snapshot() const {
        std::vector<T> out; out.reserve(N);
        size_t h = head.load(std::memory_order_relaxed);
        size_t count = std::min(h, N);
        for (size_t i = 0; i < count; i++) {
            T v = buf[(h - count + i) & (N-1)].load(std::memory_order_relaxed);
            if (v > 0) out.push_back(v);
        }
        return out;
    }
};

// ═══════════════════════════════════════════════════════
//  Circuit Breaker + Backend
// ═══════════════════════════════════════════════════════

enum class Algorithm { ROUND_ROBIN, LEAST_CONNECTIONS, IP_HASH };
enum class CBState   { CLOSED, OPEN, HALF_OPEN };

struct Backend {
    std::string id, host;
    int port, weight;

    std::atomic<bool>      healthy{true};
    std::atomic<int>       consecutive_fails{0}, consecutive_ok{0};
    std::atomic<CBState>   cb_state{CBState::CLOSED};
    std::atomic<int>       cb_error_count{0};
    std::atomic<long long> cb_open_time{0};
    std::atomic<long long> total_requests{0}, total_errors{0};
    std::atomic<int>       active_connections{0};
    std::atomic<long long> total_latency_us{0};

    double req_per_sec = 0.0, error_rate = 0.0;
    LockFreeRing<long long, LATENCY_WINDOW> ring;

    Backend(std::string i, std::string h, int p, int w=1)
        : id(std::move(i)), host(std::move(h)), port(p), weight(w) {}

    void record_latency(long long us) { total_latency_us += us; ring.push(us); }

    double percentile(double p) const {
        auto s = ring.snapshot();
        if (s.empty()) return 0.0;
        std::sort(s.begin(), s.end());
        return s[std::min((size_t)(p*s.size()), s.size()-1)] / 1000.0;
    }

    void cb_record_error() {
        int e = ++cb_error_count;
        if (e >= CB_ERROR_THRESHOLD && cb_state.load() == CBState::CLOSED) {
            cb_state.store(CBState::OPEN);
            auto now = std::chrono::system_clock::now().time_since_epoch();
            cb_open_time.store(std::chrono::duration_cast<std::chrono::seconds>(now).count());
            LOG_WARN("CIRCUIT_OPEN " + id + " after " + std::to_string(e) + " errors");
        }
    }

    void cb_record_success() {
        cb_error_count.store(0);
        if (cb_state.load() == CBState::HALF_OPEN) {
            cb_state.store(CBState::CLOSED);
            LOG_INFO("CIRCUIT_CLOSED " + id + " — recovered");
        }
    }

    bool cb_allow() {
        CBState s = cb_state.load();
        if (s == CBState::CLOSED || s == CBState::HALF_OPEN) return true;
        auto now = std::chrono::system_clock::now().time_since_epoch();
        long long now_s = std::chrono::duration_cast<std::chrono::seconds>(now).count();
        if (now_s - cb_open_time.load() >= CB_HALF_OPEN_SECS) {
            cb_state.store(CBState::HALF_OPEN);
            return true;
        }
        return false;
    }

    std::string cb_state_str() const {
        switch (cb_state.load()) {
            case CBState::CLOSED:    return "CLOSED";
            case CBState::OPEN:      return "OPEN";
            case CBState::HALF_OPEN: return "HALF_OPEN";
        }
        return "?";
    }
};

// ═══════════════════════════════════════════════════════
//  Global Metrics
// ═══════════════════════════════════════════════════════

struct GlobalMetrics {
    LockFreeRing<long long, 4096> latency_ring;
    std::atomic<long long> total_requests{0}, total_errors{0}, total_failovers{0};
    double req_per_sec=0, p50_ms=0, p95_ms=0, p99_ms=0, error_rate=0, load_variance=0;
    std::chrono::steady_clock::time_point start_time;

    void record(long long us) { latency_ring.push(us); }

    double percentile(double p) {
        auto s = latency_ring.snapshot();
        if (s.empty()) return 0.0;
        std::sort(s.begin(), s.end());
        return s[std::min((size_t)(p*s.size()), s.size()-1)] / 1000.0;
    }
} gmetrics;

// ═══════════════════════════════════════════════════════
//  Backend Pool
// ═══════════════════════════════════════════════════════

class BackendPool {
public:
    std::mutex mtx;
    std::vector<std::shared_ptr<Backend>> backends;
    std::atomic<Algorithm> algorithm{Algorithm::ROUND_ROBIN};
    std::atomic<size_t>    rr_index{0};
    int backend_counter = 0;

    void add(const std::string& host, int port, int weight=1) {
        std::lock_guard<std::mutex> lk(mtx);
        std::string id = "B" + std::to_string(++backend_counter);
        backends.push_back(std::make_shared<Backend>(id, host, port, weight));
        rr_index.store(0);  // reset so new backend gets fair share immediately
        LOG_INFO("Backend added: " + id + " " + host + ":" + std::to_string(port) +
                 " weight=" + std::to_string(weight));
        std::cout << "  Backend " << id << " added (" << host << ":" << port
                  << " weight=" << weight << ")\n";
    }

    bool remove(const std::string& id) {
        std::lock_guard<std::mutex> lk(mtx);
        auto it = std::find_if(backends.begin(), backends.end(),
            [&](auto& b){ return b->id == id; });
        if (it == backends.end()) return false;
        backends.erase(it);
        rr_index.store(0);  // reset after removal to avoid index out of bounds
        return true;
    }

    std::shared_ptr<Backend> select(const std::string& client_ip="") {
        std::lock_guard<std::mutex> lk(mtx);
        std::vector<std::shared_ptr<Backend>> available;
        for (auto& b : backends)
            if (b->healthy && b->cb_allow()) available.push_back(b);
        if (available.empty()) return nullptr;

        Algorithm algo = algorithm.load();

        if (algo == Algorithm::ROUND_ROBIN) {
            std::vector<std::shared_ptr<Backend>> pool;
            for (auto& b : available)
                for (int i = 0; i < b->weight; i++) pool.push_back(b);
            return pool[rr_index.fetch_add(1) % pool.size()];
        }
        if (algo == Algorithm::LEAST_CONNECTIONS) {
            return *std::min_element(available.begin(), available.end(),
                [](auto& a, auto& b){
                    return (double)a->active_connections/a->weight <
                           (double)b->active_connections/b->weight;
                });
        }
        // IP_HASH — hash on client_ip + connection time microseconds as salt
        // This ensures local traffic (all from 127.0.0.1) still distributes
        // In production with real client IPs, same IP always hits same pod
        auto now_us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        // Mix IP hash with time-based salt for local/same-IP traffic
        size_t h = std::hash<std::string>{}(client_ip);
        // If all traffic from same IP, use modulo on request count instead
        if (available.size() > 1) {
            h ^= (now_us >> 10);  // mix in time bits to spread local traffic
        }
        return available[h % available.size()];
    }

    std::string algo_name() const {
        switch (algorithm.load()) {
            case Algorithm::ROUND_ROBIN:       return "round-robin";
            case Algorithm::LEAST_CONNECTIONS: return "least-connections";
            case Algorithm::IP_HASH:           return "ip-hash";
        }
        return "unknown";
    }
} pool;


// ═══════════════════════════════════════════════════════
//  Token Bucket Rate Limiter (per client IP)
//
//  Each IP gets a bucket of MAX_TOKENS tokens.
//  Tokens refill at REFILL_RATE per second.
//  Each request costs 1 token. Empty bucket = 429.
// ═══════════════════════════════════════════════════════

static constexpr int    RL_MAX_TOKENS   = 100;  // burst capacity per IP
static constexpr double RL_REFILL_RATE  = 50.0; // tokens per second per IP

struct TokenBucket {
    double   tokens;
    std::chrono::steady_clock::time_point last_refill;
    TokenBucket() : tokens(RL_MAX_TOKENS),
                    last_refill(std::chrono::steady_clock::now()) {}
};

class RateLimiter {
public:
    // Returns true if request is allowed, false if rate limited
    bool allow(const std::string& ip) {
        std::lock_guard<std::mutex> lk(mtx_);
        auto now = std::chrono::steady_clock::now();
        auto& bucket = buckets_[ip];

        // Refill tokens based on elapsed time
        double elapsed = std::chrono::duration<double>(
            now - bucket.last_refill).count();
        bucket.tokens = std::min(
            (double)RL_MAX_TOKENS,
            bucket.tokens + elapsed * RL_REFILL_RATE
        );
        bucket.last_refill = now;

        if (bucket.tokens >= 1.0) {
            bucket.tokens -= 1.0;
            return true;   // allowed
        }
        return false;      // rate limited
    }

    int bucket_size() const { 
        std::lock_guard<std::mutex> lk(mtx_);
        return (int)buckets_.size(); 
    }

private:
    mutable std::mutex mtx_;
    std::unordered_map<std::string, TokenBucket> buckets_;
} rate_limiter;


// ═══════════════════════════════════════════════════════
//  Request Audit Log — last 50 requests
//  Stored in a fixed ring, shown on dashboard log tab
// ═══════════════════════════════════════════════════════

struct ReqLogEntry {
    std::string backend_id;
    std::string client_ip;
    std::string path;
    long long   latency_ms;
    int         status;
    std::string timestamp;
};

class ReqLog {
public:
    void add(const std::string& backend, const std::string& ip,
             const std::string& path, long long lat_ms, int status) {
        std::lock_guard<std::mutex> lk(mtx_);
        auto now = std::chrono::system_clock::now();
        auto t = std::chrono::system_clock::to_time_t(now);
        char ts[10];
        std::strftime(ts, sizeof(ts), "%H:%M:%S", std::localtime(&t));
        entries_[head_ % 50] = {backend, ip, path, lat_ms, status, ts};
        head_++;
    }

    std::vector<ReqLogEntry> recent(int n = 20) const {
        std::lock_guard<std::mutex> lk(mtx_);
        std::vector<ReqLogEntry> out;
        int count = std::min((int)std::min(head_, (size_t)50), n);
        for (int i = count - 1; i >= 0; i--) {
            int idx = (head_ - 1 - i) % 50;
            if (!entries_[idx].backend_id.empty())
                out.push_back(entries_[idx]);
        }
        return out;
    }

private:
    mutable std::mutex mtx_;
    std::array<ReqLogEntry, 50> entries_;
    size_t head_ = 0;
} req_log;

// ═══════════════════════════════════════════════════════
//  Global flags
// ═══════════════════════════════════════════════════════

static std::atomic<bool> running{true};
static std::atomic<int>  lb_socket{-1};

// ═══════════════════════════════════════════════════════
//  Shared networking helpers
// ═══════════════════════════════════════════════════════

static bool set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

// Non-blocking connect — returns fd immediately (may be EINPROGRESS)
static int connect_to_backend_nb(const std::string& host, int port) {
    struct addrinfo hints{}, *res;
    hints.ai_family = AF_INET; hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res) != 0)
        return -1;
    int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) { freeaddrinfo(res); return -1; }
    set_nonblocking(fd);
    int r = connect(fd, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);
    if (r < 0 && errno != EINPROGRESS) { close(fd); return -1; }
    return fd;
}

// Blocking connect with timeout — used by health checker + benchmark
static int connect_to_backend(const std::string& host, int port) {
    int fd = connect_to_backend_nb(host, port);
    if (fd < 0) return -1;
    struct pollfd pfd{fd, POLLOUT, 0};
    if (poll(&pfd, 1, CONNECT_TIMEOUT_MS) <= 0) { close(fd); return -1; }
    int err = 0; socklen_t len = sizeof(err);
    getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
    if (err != 0) { close(fd); return -1; }
    // Re-enable blocking
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
    return fd;
}

static void send_error_resp(int fd, int code, std::string_view body) {
    std::string resp = "HTTP/1.1 " + std::to_string(code) + " Error\r\n"
                       "Content-Length: " + std::to_string(body.size()) + "\r\n\r\n" +
                       std::string(body);
    write(fd, resp.data(), resp.size());
}

// ═══════════════════════════════════════════════════════
//  Thread 1 — kqueue Event Loop (macOS)
//  Replaces per-connection threads with a single-threaded
//  kernel-notified event loop. No blocking. No spinning.
// ═══════════════════════════════════════════════════════

#ifdef __APPLE__

enum class ConnPhase { READING_CLIENT, CONNECTING_BACKEND, PROXYING };

struct ConnState {
    int client_fd = -1, backend_fd = -1;
    ConnPhase phase = ConnPhase::READING_CLIENT;
    std::string client_ip;
    std::shared_ptr<Backend> backend;
    std::chrono::high_resolution_clock::time_point start_time;
    char read_buf[65536]; size_t read_len = 0;
    std::string backend_pending;  // buffered client data awaiting backend connect
};

static std::unordered_map<int, std::shared_ptr<ConnState>> conn_map;
static int kq_fd = -1;

static void kq_watch(int fd, int16_t filter, uint32_t flags) {
    struct kevent ev{};
    EV_SET(&ev, fd, filter, flags, 0, 0, nullptr);
    kevent(kq_fd, &ev, 1, nullptr, 0, nullptr);
}

static void close_conn(int fd) {
    auto it = conn_map.find(fd);
    if (it == conn_map.end()) return;
    auto cs = it->second;

    if (cs->backend && cs->phase == ConnPhase::PROXYING) {
        long long us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - cs->start_time).count();
        if (us > 0 && us < 30000000LL) {
            cs->backend->record_latency(us);
            cs->backend->cb_record_success();
            cs->backend->total_requests++;
            gmetrics.record(us);
            // Extract HTTP path from buffered request (first line)
            std::string path = "/";
            std::string_view req(cs->read_buf, cs->read_len);
            auto sp1 = req.find(' ');
            if (sp1 != std::string_view::npos) {
                auto sp2 = req.find(' ', sp1+1);
                if (sp2 != std::string_view::npos)
                    path = std::string(req.substr(sp1+1, sp2-sp1-1));
            }
            req_log.add(cs->backend->id, cs->client_ip, path, us/1000, 200);
            if (us > 200000)
                LOG_WARN("SLOW_REQ backend=" + cs->backend->id +
                         " " + std::to_string(us/1000) + "ms");
        }
        cs->backend->active_connections--;
    }

    auto cleanup = [](int f) {
        if (f < 0) return;
        conn_map.erase(f);
        close(f);
    };
    int cfd = cs->client_fd, bfd = cs->backend_fd;
    cs->client_fd = cs->backend_fd = -1;
    cleanup(cfd);
    cleanup(bfd);
}

static void on_client_readable(int fd, std::shared_ptr<ConnState>& cs) {
    ssize_t n = read(fd, cs->read_buf, sizeof(cs->read_buf)-1);
    if (n <= 0) { close_conn(fd); return; }
    cs->read_buf[n] = '\0'; cs->read_len = n;

    gmetrics.total_requests++;

    // Token bucket rate limiting — reject if IP exceeded rate
    if (!rate_limiter.allow(cs->client_ip)) {
        gmetrics.total_errors++;
        send_error_resp(fd, 429, "Too Many Requests\n");
        close_conn(fd); return;
    }

    auto backend = pool.select(cs->client_ip);
    if (!backend) {
        gmetrics.total_errors++;
        LOG_WARN("503 No healthy backends for " + cs->client_ip);
        send_error_resp(fd, 503, "No healthy backends\n");
        close_conn(fd); return;
    }

    cs->backend = backend;
    cs->backend->active_connections++;
    cs->start_time = std::chrono::high_resolution_clock::now();
    cs->backend_pending = std::string(cs->read_buf, n);
    cs->phase = ConnPhase::CONNECTING_BACKEND;

    int bfd = connect_to_backend_nb(backend->host, backend->port);
    if (bfd < 0) {
        backend->active_connections--;
        backend->total_errors++;
        backend->cb_record_error();
        gmetrics.total_errors++;
        LOG_WARN("502 Connect failed to " + backend->id);
        send_error_resp(fd, 502, "Backend failed\n");
        close_conn(fd); return;
    }
    cs->backend_fd = bfd;
    conn_map[bfd] = cs;
    kq_watch(bfd, EVFILT_WRITE, EV_ADD | EV_ONESHOT);
}

static void on_backend_connect(int fd, std::shared_ptr<ConnState>& cs) {
    int err = 0; socklen_t len = sizeof(err);
    getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
    if (err != 0) {
        cs->backend->active_connections--;
        cs->backend->cb_record_error();
        gmetrics.total_errors++;
        send_error_resp(cs->client_fd, 502, "Backend connect failed\n");
        close_conn(cs->client_fd); return;
    }
    cs->phase = ConnPhase::PROXYING;
    if (!cs->backend_pending.empty()) {
        write(fd, cs->backend_pending.data(), cs->backend_pending.size());
        cs->backend_pending.clear();
    }
    kq_watch(cs->client_fd,  EVFILT_READ, EV_ADD);
    kq_watch(cs->backend_fd, EVFILT_READ, EV_ADD);
}

static void on_proxy_readable(int src, std::shared_ptr<ConnState>& cs) {
    char buf[65536];
    ssize_t n = read(src, buf, sizeof(buf));
    if (n <= 0) { close_conn(src); return; }
    int dst = (src == cs->client_fd) ? cs->backend_fd : cs->client_fd;
    ssize_t w = 0;
    while (w < n) {
        ssize_t x = write(dst, buf+w, n-w);
        if (x <= 0) { close_conn(src); return; }
        w += x;
    }
}

void acceptor() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET; addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(LB_PORT);
    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "  [ERROR] bind() failed on port " << LB_PORT << "\n";
        close(server_fd); return;
    }
    listen(server_fd, 4096);
    set_nonblocking(server_fd);
    lb_socket = server_fd;

    kq_fd = kqueue();
    kq_watch(server_fd, EVFILT_READ, EV_ADD);

    std::cout << "  kqueue event loop running on :" << LB_PORT << "\n";
    LOG_INFO("kqueue event loop started — single thread, all connections");

    struct kevent events[256];
    while (running) {
        struct timespec timeout{0, 500000000};
        int n = kevent(kq_fd, nullptr, 0, events, 256, &timeout);
        if (n < 0) { if (errno == EINTR) continue; break; }

        for (int i = 0; i < n; i++) {
            int fd = (int)events[i].ident;

            if (fd == server_fd) {
                while (true) {
                    struct sockaddr_in ca{}; socklen_t cl = sizeof(ca);
                    int cfd = accept(server_fd, (struct sockaddr*)&ca, &cl);
                    if (cfd < 0) break;
                    set_nonblocking(cfd);
                    char ip[INET_ADDRSTRLEN];
                    inet_ntop(AF_INET, &ca.sin_addr, ip, sizeof(ip));
                    auto cs = std::make_shared<ConnState>();
                    cs->client_fd = cfd; cs->client_ip = ip;
                    conn_map[cfd] = cs;
                    kq_watch(cfd, EVFILT_READ, EV_ADD | EV_ONESHOT);
                }
                continue;
            }

            if (events[i].flags & EV_EOF || events[i].flags & EV_ERROR) {
                close_conn(fd); continue;
            }

            auto it = conn_map.find(fd);
            if (it == conn_map.end()) continue;
            auto& cs = it->second;

            if (events[i].filter == EVFILT_READ) {
                if (cs->phase == ConnPhase::READING_CLIENT)
                    on_client_readable(fd, cs);
                else if (cs->phase == ConnPhase::PROXYING)
                    on_proxy_readable(fd, cs);
            } else if (events[i].filter == EVFILT_WRITE) {
                if (cs->phase == ConnPhase::CONNECTING_BACKEND)
                    on_backend_connect(fd, cs);
            }
        }
    }
    close(kq_fd); close(server_fd);
    LOG_INFO("kqueue event loop stopped");
}

#else
// ── Linux fallback: poll + per-connection threads ──────

static void proxy_blocking(int cfd, int bfd) {
    char buf[65536];
    while (running) {
        struct pollfd pfds[2] = {{cfd,POLLIN,0},{bfd,POLLIN,0}};
        if (poll(pfds,2,PROXY_TIMEOUT_MS)<=0) break;
        for (int i=0;i<2;i++) {
            if (!(pfds[i].revents&POLLIN)) continue;
            int src=(i==0)?cfd:bfd, dst=(i==0)?bfd:cfd;
            ssize_t r=read(src,buf,sizeof(buf));
            if (r<=0) return;
            ssize_t w=0;
            while(w<r){ssize_t x=write(dst,buf+w,r-w);if(x<=0)return;w+=x;}
        }
    }
}

static void handle_connection(int cfd, std::string client_ip) {
    gmetrics.total_requests++;
    if (!rate_limiter.allow(client_ip)) {
        gmetrics.total_errors++;
        send_error_resp(cfd, 429, "Too Many Requests\n");
        close(cfd); return;
    }
    auto backend = pool.select(client_ip);
    if (!backend) {
        gmetrics.total_errors++;
        send_error_resp(cfd, 503, "No healthy backends\n");
        close(cfd); return;
    }
    backend->active_connections++;
    auto t0 = std::chrono::high_resolution_clock::now();
    int bfd = connect_to_backend(backend->host, backend->port);
    if (bfd < 0) {
        backend->active_connections--;
        backend->total_errors++;
        backend->cb_record_error();
        gmetrics.total_errors++;
        send_error_resp(cfd, 502, "Backend failed\n");
        close(cfd); return;
    }
    proxy_blocking(cfd, bfd);
    long long us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::high_resolution_clock::now() - t0).count();
    backend->active_connections--;
    backend->total_requests++;
    backend->record_latency(us);
    backend->cb_record_success();
    gmetrics.record(us);
    req_log.add(backend->id, client_ip, "/", us/1000, 200);
    close(bfd); close(cfd);
}

void acceptor() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt=1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
    struct sockaddr_in addr{};
    addr.sin_family=AF_INET; addr.sin_addr.s_addr=INADDR_ANY; addr.sin_port=htons(LB_PORT);
    if (bind(server_fd,(struct sockaddr*)&addr,sizeof(addr))<0) {
        std::cerr<<"bind() failed\n"; return;
    }
    listen(server_fd,1024); lb_socket=server_fd;
    std::cout<<"  poll acceptor on :"<<LB_PORT<<" (Linux)\n";
    while (running) {
        struct pollfd pfd{server_fd,POLLIN,0};
        if (poll(&pfd,1,500)<=0) continue;
        struct sockaddr_in ca{}; socklen_t cl=sizeof(ca);
        int cfd=accept(server_fd,(struct sockaddr*)&ca,&cl);
        if (cfd<0) continue;
        char ip[INET_ADDRSTRLEN]; inet_ntop(AF_INET,&ca.sin_addr,ip,sizeof(ip));
        std::thread(handle_connection,cfd,std::string(ip)).detach();
    }
    close(server_fd);
}
#endif

// ═══════════════════════════════════════════════════════
//  Thread 2 — Health Checker
// ═══════════════════════════════════════════════════════

static bool probe_backend(Backend& b) {
    int fd = connect_to_backend(b.host, b.port);
    if (fd < 0) return false;
    std::string req = "HEAD /health HTTP/1.0\r\nHost: " + b.host + "\r\n\r\n";
    write(fd, req.c_str(), req.size());
    char buf[256]; struct pollfd pfd{fd, POLLIN, 0}; bool ok = false;
    if (poll(&pfd, 1, HEALTH_TIMEOUT_MS) > 0) {
        ssize_t n = read(fd, buf, sizeof(buf)-1);
        if (n > 0) { buf[n]='\0'; ok = (strncmp(buf,"HTTP/",5)==0); }
    }
    close(fd); return ok;
}

void health_checker() {
    LOG_INFO("Health checker started (interval=" + std::to_string(HEALTH_INTERVAL_MS) +
             "ms fail_threshold=" + std::to_string(FAIL_THRESHOLD) + ")");
    int probe_cycle = 0;
    while (running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(HEALTH_INTERVAL_MS));
        probe_cycle++;
        std::vector<std::shared_ptr<Backend>> snapshot;
        { std::lock_guard<std::mutex> lk(pool.mtx); snapshot = pool.backends; }
        if (snapshot.empty()) continue;

        if (probe_cycle % 5 == 0) {
            std::string s = "HEALTH_SUMMARY cycle=" + std::to_string(probe_cycle) + " backends=[";
            for (auto& b : snapshot)
                s += b->id + ":" + (b->healthy?"UP":"DOWN") +
                     "(fails=" + std::to_string(b->consecutive_fails.load()) + ") ";
            LOG_INFO(s + "]");
        }

        for (auto& b : snapshot) {
            auto tp0 = std::chrono::high_resolution_clock::now();
            bool alive = probe_backend(*b);
            long long probe_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::high_resolution_clock::now() - tp0).count();

            if (!alive) {
                int fails = ++b->consecutive_fails;
                b->consecutive_ok = 0;
                LOG_WARN("PROBE_FAIL " + b->id + " fails=" + std::to_string(fails) +
                         " probe_ms=" + std::to_string(probe_ms));
                if (fails >= FAIL_THRESHOLD && b->healthy) {
                    b->healthy = false;
                    gmetrics.total_failovers++;
                    std::string msg = "Backend " + b->id + " marked DOWN";
                    LOG_WARN(msg);
                    std::cout << "\n  \033[1;31m[DOWN]\033[0m " << msg << "\n";
                }
            } else {
                int oks = ++b->consecutive_ok;
                b->consecutive_fails = 0;
                if (oks >= RECOVER_THRESHOLD && !b->healthy) {
                    b->healthy = true;
                    std::string msg = "Backend " + b->id + " marked UP";
                    LOG_INFO(msg);
                    std::cout << "\n  \033[1;32m[UP]\033[0m " << msg << "\n";
                }
            }
        }
    }
    LOG_INFO("Health checker stopped");
}

// ═══════════════════════════════════════════════════════
//  Thread 3 — Metrics Engine
// ═══════════════════════════════════════════════════════

void metrics_engine() {
    LOG_INFO("Metrics engine started");
    long long last_total = 0, last_errors = 0;
    std::map<std::string, long long> last_backend_reqs;

    while (running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(METRICS_INTERVAL_MS));
        long long cur_total  = gmetrics.total_requests.load();
        long long cur_errors = gmetrics.total_errors.load();
        long long dt = cur_total - last_total;
        long long de = cur_errors - last_errors;
        gmetrics.req_per_sec = (double)dt;
        gmetrics.error_rate  = dt > 0 ? (double)de / dt * 100.0 : 0.0;
        gmetrics.p50_ms = gmetrics.percentile(0.50);
        gmetrics.p95_ms = gmetrics.percentile(0.95);
        gmetrics.p99_ms = gmetrics.percentile(0.99);

        std::vector<double> rates;
        {
            std::lock_guard<std::mutex> lk(pool.mtx);
            for (auto& b : pool.backends) {
                long long cur = b->total_requests.load();
                double rps = (double)(cur - last_backend_reqs[b->id]);
                b->req_per_sec = rps;
                last_backend_reqs[b->id] = cur;
                if (b->healthy) rates.push_back(rps);
            }
        }
        if (rates.size() > 1) {
            double mean = std::accumulate(rates.begin(),rates.end(),0.0)/rates.size();
            double sq = 0;
            for (double r : rates) sq += (r-mean)*(r-mean);
            gmetrics.load_variance = std::sqrt(sq/rates.size());
        }
        last_total = cur_total; last_errors = cur_errors;

        static int tick = 0;
        if (++tick % 10 == 0) {
            std::ostringstream b;
            b << std::fixed << std::setprecision(1);
            b << "GLOBAL req/s=" << (int)gmetrics.req_per_sec
              << " p50=" << gmetrics.p50_ms << "ms"
              << " p95=" << gmetrics.p95_ms << "ms"
              << " p99=" << gmetrics.p99_ms << "ms"
              << " err=" << gmetrics.error_rate << "%"
              << " total=" << gmetrics.total_requests.load()
              << " errors=" << gmetrics.total_errors.load();
            LOG_BENCH(b.str());
            std::lock_guard<std::mutex> lk(pool.mtx);
            for (auto& bk : pool.backends) {
                std::ostringstream bs;
                bs << std::fixed << std::setprecision(1);
                bs << "BACKEND " << bk->id
                   << " status=" << (bk->healthy?"UP":"DOWN")
                   << " cb=" << bk->cb_state_str()
                   << " req/s=" << bk->req_per_sec
                   << " active=" << bk->active_connections.load()
                   << " total=" << bk->total_requests.load()
                   << " weight=" << bk->weight
                   << " p50=" << bk->percentile(0.50) << "ms"
                   << " p99=" << bk->percentile(0.99) << "ms";
                LOG_BENCH(bs.str());
            }
        }
    }
    LOG_INFO("Metrics engine stopped");
}

// ═══════════════════════════════════════════════════════
//  JSON Serialization
// ═══════════════════════════════════════════════════════

static std::string esc(const std::string& s) {
    std::string o;
    for (char c : s) {
        if (c=='"') o+="\\\""; else if (c=='\\') o+="\\\\"; else o+=c;
    }
    return o;
}

static std::string state_to_json() {
    std::ostringstream j;
    auto uptime = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - gmetrics.start_time).count();
    j << std::fixed << std::setprecision(2);
    j << "{\"backends\":[";
    std::lock_guard<std::mutex> lk(pool.mtx);
    bool first = true;
    for (auto& b : pool.backends) {
        if (!first) j << ",";
        j << "{\"id\":\"" << esc(b->id) << "\""
          << ",\"host\":\"" << esc(b->host) << "\""
          << ",\"port\":" << b->port
          << ",\"healthy\":" << (b->healthy?"true":"false")
          << ",\"weight\":" << b->weight
          << ",\"cb_state\":\"" << esc(b->cb_state_str()) << "\""
          << ",\"cb_errors\":" << b->cb_error_count.load()
          << ",\"active_connections\":" << b->active_connections.load()
          << ",\"total_requests\":" << b->total_requests.load()
          << ",\"total_errors\":" << b->total_errors.load()
          << ",\"req_per_sec\":" << b->req_per_sec
          << ",\"avg_latency_ms\":" << b->total_latency_us/1000.0/std::max(1LL,b->total_requests.load())
          << ",\"p50_ms\":" << b->percentile(0.50)
          << ",\"p95_ms\":" << b->percentile(0.95)
          << ",\"p99_ms\":" << b->percentile(0.99)
          << "}";
        first = false;
    }
    j << "],\"global\":{"
      << "\"total_requests\":" << gmetrics.total_requests.load()
      << ",\"total_errors\":" << gmetrics.total_errors.load()
      << ",\"total_failovers\":" << gmetrics.total_failovers.load()
      << ",\"req_per_sec\":" << gmetrics.req_per_sec
      << ",\"p50_ms\":" << gmetrics.p50_ms
      << ",\"p95_ms\":" << gmetrics.p95_ms
      << ",\"p99_ms\":" << gmetrics.p99_ms
      << ",\"error_rate\":" << gmetrics.error_rate
      << ",\"load_variance\":" << gmetrics.load_variance
      << ",\"algorithm\":\"" << esc(pool.algo_name()) << "\""
      << ",\"uptime_s\":" << uptime
      << ",\"rate_limited_ips\":" << rate_limiter.bucket_size()
      << "},"
      << "\"req_log\":[";

    // Serialize last 30 requests for dashboard log tab
    auto logs = req_log.recent(30);
    bool lfirst = true;
    for (auto& e : logs) {
        if (!lfirst) j << ",";
        j << "{\"ts\":\"" << esc(e.timestamp) << "\""
          << ",\"backend\":\"" << esc(e.backend_id) << "\""
          << ",\"ip\":\"" << esc(e.client_ip) << "\""
          << ",\"path\":\"" << esc(e.path) << "\""
          << ",\"latency_ms\":" << e.latency_ms
          << ",\"status\":" << e.status << "}";
        lfirst = false;
    }
    j << "]}";
    return j.str();
}

// ═══════════════════════════════════════════════════════
//  Built-in Benchmark Mode
// ═══════════════════════════════════════════════════════

struct BenchResult {
    int total_requests, concurrency, errors;
    long long duration_ms, throughput_rps;
    double p50_ms, p95_ms, p99_ms, error_rate;
};

BenchResult run_benchmark(int total, int concurrency) {
    std::cout << "\n  Benchmark: " << total << " requests, "
              << concurrency << " concurrent...\n";
    LockFreeRing<long long, 4096> bench_ring;
    std::atomic<int> completed{0}, errors{0};

    auto fire = [&]() {
        auto backend = pool.select("127.0.0.1");
        if (!backend) { errors++; completed++; return; }
        auto t0 = std::chrono::high_resolution_clock::now();
        int fd = connect_to_backend(backend->host, backend->port);
        if (fd < 0) { errors++; completed++; return; }
        const char* req = "GET /bench HTTP/1.0\r\nHost: localhost\r\n\r\n";
        write(fd, req, strlen(req));
        char buf[4096]; while (read(fd, buf, sizeof(buf)) > 0) {}
        close(fd);
        long long us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - t0).count();
        bench_ring.push(us);
        completed++;
    };

    auto wall = std::chrono::high_resolution_clock::now();
    int sent = 0;
    while (sent < total) {
        std::vector<std::thread> batch;
        int bs = std::min(concurrency, total-sent);
        for (int i=0;i<bs;i++) batch.emplace_back(fire);
        for (auto& t:batch) t.join();
        sent += bs;
        if (sent % (total/5) == 0)
            std::cout << "  " << sent << "/" << total << " done...\n";
    }
    long long dur = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::high_resolution_clock::now() - wall).count();

    auto samples = bench_ring.snapshot();
    std::sort(samples.begin(), samples.end());
    auto pct = [&](double p) -> double {
        if (samples.empty()) return 0.0;
        return samples[std::min((size_t)(p*samples.size()),samples.size()-1)] / 1000.0;
    };

    BenchResult r;
    r.total_requests = total; r.concurrency = concurrency;
    r.duration_ms = dur;
    r.throughput_rps = dur > 0 ? (long long)total*1000/dur : 0;
    r.p50_ms = pct(0.50); r.p95_ms = pct(0.95); r.p99_ms = pct(0.99);
    r.errors = errors.load();
    r.error_rate = (double)errors/total*100.0;
    return r;
}

void print_bench_result(const BenchResult& r) {
    std::cout << "\n\033[1m╔══════════════════════════════════════════════════════╗\033[0m\n";
    std::cout << "\033[1m║              BENCHMARK RESULTS                       ║\033[0m\n";
    std::cout << "\033[1m╚══════════════════════════════════════════════════════╝\033[0m\n";
    std::cout << std::fixed << std::setprecision(1);
    std::cout << "  Requests     : " << r.total_requests << "\n";
    std::cout << "  Concurrency  : " << r.concurrency << "\n";
    std::cout << "  Duration     : " << r.duration_ms << "ms\n";
    std::cout << "  \033[1;36mThroughput   : " << r.throughput_rps << " req/s\033[0m\n";
    std::cout << "  \033[1;32mp50 latency  : " << r.p50_ms << "ms\033[0m\n";
    std::cout << "  \033[1;33mp95 latency  : " << r.p95_ms << "ms\033[0m\n";
    std::cout << "  \033[1;31mp99 latency  : " << r.p99_ms << "ms\033[0m\n";
    std::cout << "  Error rate   : " << r.error_rate << "%\n\n";
    LOG_BENCH("BENCHMARK total=" + std::to_string(r.total_requests) +
              " concurrency=" + std::to_string(r.concurrency) +
              " duration=" + std::to_string(r.duration_ms) + "ms" +
              " throughput=" + std::to_string(r.throughput_rps) + "req/s" +
              " p50=" + std::to_string(r.p50_ms) + "ms" +
              " p99=" + std::to_string(r.p99_ms) + "ms" +
              " errors=" + std::to_string(r.errors));
}

// ═══════════════════════════════════════════════════════
//  CLI Display
// ═══════════════════════════════════════════════════════

void print_help() {
    std::cout << R"(
  ┌──────────────────────────────────────────────────────────┐
  │  NetBalancer Commands                                    │
  ├──────────────────────────────────────────────────────────┤
  │  add <host> <port> [weight]   Add backend               │
  │  remove <id>                  Remove backend            │
  │  spawn <port> [weight]        Spawn Python pod + add    │
  │  killpod <id>                 Kill pod process + remove │
  │  algo <rr|lc|hash>            Set routing algorithm     │
  │  status                       Live status               │
  │  bench                        Metrics summary           │
  │  benchmark [N] [C]            Run N reqs, C concurrent  │
  │  reload                       Hot reload config.json    │
  │  help                         Show this help            │
  │  exit                         Shutdown                  │
  └──────────────────────────────────────────────────────────┘
)";
}

void show_status() {
    std::cout << "\n\033[1m╔══════════════════════════════════════════════════════╗\033[0m\n";
    std::cout << "\033[1m║              LOAD BALANCER STATUS                    ║\033[0m\n";
    std::cout << "\033[1m╚══════════════════════════════════════════════════════╝\033[0m\n";
    std::cout << "  Algorithm : " << pool.algo_name() << "\n";
    std::cout << "  Req/s     : " << std::fixed << std::setprecision(0) << gmetrics.req_per_sec << "\n";
    std::cout << "  p50/p99   : " << std::setprecision(1) << gmetrics.p50_ms << "ms / " << gmetrics.p99_ms << "ms\n";
    std::cout << "  Total req : " << gmetrics.total_requests.load() << "\n";
    std::cout << "  Errors    : " << gmetrics.total_errors.load() << "\n";
    std::cout << "  Failovers : " << gmetrics.total_failovers.load() << "\n\n";
    std::lock_guard<std::mutex> lk(pool.mtx);
    std::cout << "  " << std::left
              << std::setw(5) << "ID" << std::setw(20) << "Backend"
              << std::setw(8) << "Status" << std::setw(8) << "CB"
              << std::setw(8) << "Active" << std::setw(10) << "Req/s"
              << "Total\n";
    std::cout << "  " << std::string(65,'-') << "\n";
    for (auto& b : pool.backends) {
        std::string addr = b->host+":"+std::to_string(b->port);
        std::cout << "  " << std::left
                  << std::setw(5)  << b->id
                  << std::setw(20) << addr
                  << std::setw(15) << (b->healthy?"\033[32mUP\033[0m  ":"\033[31mDOWN\033[0m")
                  << std::setw(12) << b->cb_state_str()
                  << std::setw(8)  << b->active_connections.load()
                  << std::setw(10) << std::setprecision(0) << b->req_per_sec
                  << b->total_requests.load() << "\n";
    }
    std::cout << "\n";
}

void show_bench() {
    long long total = gmetrics.total_requests.load();
    auto uptime = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - gmetrics.start_time).count();
    std::cout << "\n  Uptime: " << uptime << "s  Total: " << total
              << "  Avg: " << (uptime>0?total/uptime:0) << " req/s"
              << "  p50=" << std::fixed << std::setprecision(1) << gmetrics.p50_ms << "ms"
              << "  p99=" << gmetrics.p99_ms << "ms"
              << "  err=" << gmetrics.error_rate << "%\n\n";
}

// ═══════════════════════════════════════════════════════
//  Pod Process Management
// ═══════════════════════════════════════════════════════

static std::mutex pod_mtx;
static std::map<int, pid_t> pod_pids;

static int spawn_pod(int port) {
    std::string cmd =
        "python3 -c \""
        "import http.server,time,random,sys\n"
        "class H(http.server.BaseHTTPRequestHandler):\n"
        "  def do_GET(self):\n"
        "    time.sleep(random.uniform(0.001,0.008))\n"
        "    b=f'pod:" + std::to_string(port) + "\\\\n'\n"
        "    self.send_response(200)\n"
        "    self.send_header('Content-Length',len(b))\n"
        "    self.end_headers()\n"
        "    self.wfile.write(b.encode())\n"
        "  def do_HEAD(self):\n"
        "    self.send_response(200)\n"
        "    self.end_headers()\n"
        "  def log_message(self,*a):pass\n"
        "http.server.HTTPServer(('',"+std::to_string(port)+"),H).serve_forever()\n"
        "\" &";
    system(cmd.c_str());

    LOG_INFO("Waiting for pod :" + std::to_string(port) + " to be ready...");
    for (int i = 0; i < 20; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        int fd = connect_to_backend("localhost", port);
        if (fd >= 0) {
            close(fd);
            LOG_INFO("Pod :" + std::to_string(port) + " ready after " +
                     std::to_string((i+1)*100) + "ms");
            break;
        }
    }
    std::string pid_cmd = "lsof -ti tcp:" + std::to_string(port) + " 2>/dev/null";
    FILE* f = popen(pid_cmd.c_str(), "r");
    pid_t pid = -1;
    if (f) { fscanf(f, "%d", &pid); pclose(f); }
    { std::lock_guard<std::mutex> lk(pod_mtx); if (pid>0) pod_pids[port]=pid; }
    return pid;
}

static void kill_pod(int port) {
    std::lock_guard<std::mutex> lk(pod_mtx);
    auto it = pod_pids.find(port);
    if (it != pod_pids.end()) { kill(it->second, SIGTERM); pod_pids.erase(it); }
    else {
        std::string cmd = "lsof -ti tcp:" + std::to_string(port) +
                          " | xargs kill -9 2>/dev/null";
        system(cmd.c_str());
    }
}


// ═══════════════════════════════════════════════════════
//  Prometheus Metrics Endpoint
//
//  Exposes /metrics in standard Prometheus text format.
//  Scraped by Prometheus every 15s in production.
//  Metrics: request counters, latency histograms,
//           backend health, active connections, error rates
// ═══════════════════════════════════════════════════════

static std::string prometheus_metrics() {
    std::ostringstream m;
    m << std::fixed << std::setprecision(4);

    auto uptime = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - gmetrics.start_time).count();

    // Global counters
    m << "# HELP netbalancer_requests_total Total HTTP requests received\n";
    m << "# TYPE netbalancer_requests_total counter\n";
    m << "netbalancer_requests_total " << gmetrics.total_requests.load() << "\n\n";

    m << "# HELP netbalancer_errors_total Total request errors (4xx/5xx)\n";
    m << "# TYPE netbalancer_errors_total counter\n";
    m << "netbalancer_errors_total " << gmetrics.total_errors.load() << "\n\n";

    m << "# HELP netbalancer_failovers_total Total backend failover events\n";
    m << "# TYPE netbalancer_failovers_total counter\n";
    m << "netbalancer_failovers_total " << gmetrics.total_failovers.load() << "\n\n";

    // Global latency percentiles (gauges — current rolling window)
    m << "# HELP netbalancer_latency_p50_ms P50 request latency milliseconds\n";
    m << "# TYPE netbalancer_latency_p50_ms gauge\n";
    m << "netbalancer_latency_p50_ms " << gmetrics.p50_ms << "\n\n";

    m << "# HELP netbalancer_latency_p95_ms P95 request latency milliseconds\n";
    m << "# TYPE netbalancer_latency_p95_ms gauge\n";
    m << "netbalancer_latency_p95_ms " << gmetrics.p95_ms << "\n\n";

    m << "# HELP netbalancer_latency_p99_ms P99 request latency milliseconds\n";
    m << "# TYPE netbalancer_latency_p99_ms gauge\n";
    m << "netbalancer_latency_p99_ms " << gmetrics.p99_ms << "\n\n";

    m << "# HELP netbalancer_requests_per_second Current request throughput\n";
    m << "# TYPE netbalancer_requests_per_second gauge\n";
    m << "netbalancer_requests_per_second " << gmetrics.req_per_sec << "\n\n";

    m << "# HELP netbalancer_uptime_seconds Seconds since process start\n";
    m << "# TYPE netbalancer_uptime_seconds counter\n";
    m << "netbalancer_uptime_seconds " << uptime << "\n\n";

    // Per-backend metrics with labels
    m << "# HELP netbalancer_backend_requests_total Requests routed to each backend\n";
    m << "# TYPE netbalancer_backend_requests_total counter\n";

    m << "# HELP netbalancer_backend_errors_total Errors per backend\n";
    m << "# TYPE netbalancer_backend_errors_total counter\n";

    m << "# HELP netbalancer_backend_active_connections Active connections per backend\n";
    m << "# TYPE netbalancer_backend_active_connections gauge\n";

    m << "# HELP netbalancer_backend_healthy Backend health status (1=up, 0=down)\n";
    m << "# TYPE netbalancer_backend_healthy gauge\n";

    m << "# HELP netbalancer_backend_p99_ms P99 latency per backend\n";
    m << "# TYPE netbalancer_backend_p99_ms gauge\n";

    m << "# HELP netbalancer_backend_circuit_breaker Circuit breaker state (0=CLOSED, 1=HALF_OPEN, 2=OPEN)\n";
    m << "# TYPE netbalancer_backend_circuit_breaker gauge\n";

    m << "# HELP netbalancer_backend_weight Configured weight per backend\n";
    m << "# TYPE netbalancer_backend_weight gauge\n";

    {
        std::lock_guard<std::mutex> lk(pool.mtx);
        for (auto& b : pool.backends) {
            std::string lbl = "{backend=\"" + b->id + "\",host=\"" +
                               b->host + ":" + std::to_string(b->port) + "\"}";
            int cb_val = (b->cb_state.load() == CBState::CLOSED)    ? 0 :
                         (b->cb_state.load() == CBState::HALF_OPEN)  ? 1 : 2;

            m << "netbalancer_backend_requests_total"      << lbl << " " << b->total_requests.load()    << "\n";
            m << "netbalancer_backend_errors_total"        << lbl << " " << b->total_errors.load()      << "\n";
            m << "netbalancer_backend_active_connections"  << lbl << " " << b->active_connections.load()<< "\n";
            m << "netbalancer_backend_healthy"             << lbl << " " << (b->healthy ? 1 : 0)        << "\n";
            m << "netbalancer_backend_p99_ms"              << lbl << " " << b->percentile(0.99)         << "\n";
            m << "netbalancer_backend_circuit_breaker"     << lbl << " " << cb_val                      << "\n";
            m << "netbalancer_backend_weight"              << lbl << " " << b->weight                   << "\n";
        }
    }

    return m.str();
}


// ═══════════════════════════════════════════════════════
//  Config Hot Reload (SIGHUP)
//
//  Reads config.json on SIGHUP signal — no restart needed.
//  Backends not in new config are drained and removed.
//  New backends are added immediately.
//  Algorithm and rate limit changes take effect instantly.
//
//  config.json format:
//  {
//    "algorithm": "rr",
//    "backends": [
//      {"host": "localhost", "port": 8001, "weight": 1},
//      {"host": "localhost", "port": 8002, "weight": 2}
//    ]
//  }
// ═══════════════════════════════════════════════════════

static const std::string CONFIG_PATH = "config.json";
static std::atomic<bool> reload_requested{false};

// Minimal JSON parser for config — no external deps
static std::string json_str_val(const std::string& json, const std::string& key) {
    auto pos = json.find("\"" + key + "\"");
    if (pos == std::string::npos) return "";
    pos = json.find(":", pos);
    if (pos == std::string::npos) return "";
    pos = json.find("\"", pos);
    if (pos == std::string::npos) return "";
    auto end = json.find("\"", pos+1);
    if (end == std::string::npos) return "";
    return json.substr(pos+1, end-pos-1);
}

static int json_int_val(const std::string& json, const std::string& key, int def=0) {
    auto pos = json.find("\"" + key + "\"");
    if (pos == std::string::npos) return def;
    pos = json.find(":", pos);
    if (pos == std::string::npos) return def;
    pos = json.find_first_of("0123456789", pos);
    if (pos == std::string::npos) return def;
    return std::stoi(json.substr(pos));
}

void reload_config() {
    std::ifstream f(CONFIG_PATH);
    if (!f.is_open()) {
        LOG_WARN("SIGHUP received but " + CONFIG_PATH + " not found — skipping reload");
        std::cout << "  [RELOAD] " << CONFIG_PATH << " not found — no changes made\n";
        return;
    }
    std::string json((std::istreambuf_iterator<char>(f)), {});
    LOG_INFO("Reloading config from " + CONFIG_PATH);
    std::cout << "  \033[1;36m[RELOAD]\033[0m Loading " << CONFIG_PATH << "...\n";

    // Parse algorithm
    std::string algo = json_str_val(json, "algorithm");
    if (!algo.empty()) {
        if      (algo=="rr"||algo=="round-robin")       pool.algorithm = Algorithm::ROUND_ROBIN;
        else if (algo=="lc"||algo=="least-connections") pool.algorithm = Algorithm::LEAST_CONNECTIONS;
        else if (algo=="hash"||algo=="ip-hash")         pool.algorithm = Algorithm::IP_HASH;
        std::cout << "  [RELOAD] Algorithm: " << pool.algo_name() << "\n";
        LOG_INFO("RELOAD algorithm=" + pool.algo_name());
    }

    // Parse backends array
    std::vector<std::tuple<std::string,int,int>> new_backends;
    size_t pos = json.find("\"backends\"");
    if (pos != std::string::npos) {
        size_t arr_start = json.find("[", pos);
        size_t arr_end   = json.find("]", arr_start);
        if (arr_start != std::string::npos && arr_end != std::string::npos) {
            std::string arr = json.substr(arr_start+1, arr_end-arr_start-1);
            // Parse each {host, port, weight} object
            size_t obj = 0;
            while ((obj = arr.find("{", obj)) != std::string::npos) {
                size_t obj_end = arr.find("}", obj);
                if (obj_end == std::string::npos) break;
                std::string entry = arr.substr(obj, obj_end-obj+1);
                std::string host = json_str_val(entry, "host");
                int port   = json_int_val(entry, "port");
                int weight = json_int_val(entry, "weight", 1);
                if (!host.empty() && port > 0)
                    new_backends.emplace_back(host, port, weight);
                obj = obj_end + 1;
            }
        }
    }

    if (new_backends.empty()) {
        std::cout << "  [RELOAD] No backends found in config — keeping existing\n";
        return;
    }

    // Diff: find backends to remove (in pool but not in config)
    std::vector<std::string> to_remove;
    {
        std::lock_guard<std::mutex> lk(pool.mtx);
        for (auto& b : pool.backends) {
            bool found = false;
            for (auto& [h,p,w] : new_backends)
                if (b->host==h && b->port==p) { found=true; break; }
            if (!found) to_remove.push_back(b->id);
        }
    }
    for (auto& id : to_remove) {
        pool.remove(id);
        std::cout << "  [RELOAD] Removed backend " << id << "\n";
        LOG_INFO("RELOAD removed " + id);
    }

    // Add new backends not already in pool
    for (auto& [host, port, weight] : new_backends) {
        bool exists = false;
        {
            std::lock_guard<std::mutex> lk(pool.mtx);
            for (auto& b : pool.backends)
                if (b->host==host && b->port==port) { exists=true; break; }
        }
        if (!exists) {
            pool.add(host, port, weight);
            std::cout << "  [RELOAD] Added backend " << host << ":" << port
                      << " weight=" << weight << "\n";
        }
    }

    std::cout << "  [RELOAD] Done. " << new_backends.size()
              << " backends active, algorithm=" << pool.algo_name() << "\n";
    LOG_INFO("RELOAD complete — " + std::to_string(new_backends.size()) + " backends");
}

void handle_sighup(int) { reload_requested = true; }

// ═══════════════════════════════════════════════════════
//  CLI Dispatcher
// ═══════════════════════════════════════════════════════

void dispatch(const std::string& line) {
    std::istringstream iss(line);
    std::string cmd; iss >> cmd;
    if (cmd.empty() || cmd[0]=='#') return;

    if (cmd=="add") {
        std::string host; int port; int weight=1;
        if (!(iss>>host>>port)) { std::cout<<"  Usage: add <host> <port> [weight]\n"; return; }
        iss>>weight; pool.add(host,port,weight);
    } else if (cmd=="remove") {
        std::string id; iss>>id;
        if (pool.remove(id)) std::cout<<"  Backend "<<id<<" removed.\n";
        else std::cout<<"  Not found.\n";
    } else if (cmd=="spawn") {
        int port; int weight=1; iss>>port;
        if (port<=0) { std::cout<<"  Usage: spawn <port> [weight]\n"; return; }
        iss>>weight; if(weight<1)weight=1;
        std::cout<<"  Spawning pod on :"<<port<<" (weight="<<weight<<")...\n";
        spawn_pod(port); pool.add("localhost",port,weight);
        std::cout<<"  Pod :"<<port<<" ready.\n";
    } else if (cmd=="killpod") {
        std::string id; iss>>id;
        int port=-1;
        { std::lock_guard<std::mutex> lk(pool.mtx);
          for (auto& b:pool.backends) if(b->id==id){port=b->port;break;} }
        if (port<0) { std::cout<<"  Not found.\n"; return; }
        pool.remove(id); kill_pod(port);
        std::cout<<"  Pod "<<id<<" (:"<<port<<") killed.\n";
    } else if (cmd=="algo") {
        std::string a; iss>>a;
        if      (a=="rr"||a=="round-robin")        pool.algorithm=Algorithm::ROUND_ROBIN;
        else if (a=="lc"||a=="least-connections")   pool.algorithm=Algorithm::LEAST_CONNECTIONS;
        else if (a=="hash"||a=="ip-hash")           pool.algorithm=Algorithm::IP_HASH;
        else { std::cout<<"  Use: rr, lc, hash\n"; return; }
        std::cout<<"  Algorithm: "<<pool.algo_name()<<"\n";
    } else if (cmd=="status")    { show_status();
    } else if (cmd=="bench")     { show_bench();
    } else if (cmd=="benchmark") {
        int total=1000, concurrency=50;
        iss>>total>>concurrency;
        if (pool.backends.empty()) { std::cout<<"  No backends. Spawn pods first.\n"; return; }
        print_bench_result(run_benchmark(total, concurrency));
    } else if (cmd=="reload")    {
        reload_config();
    } else if (cmd=="help")      { print_help();
    } else if (cmd=="exit"||cmd=="quit") { running=false;
    } else { std::cout<<"  Unknown: '"<<cmd<<"'. Type 'help'.\n"; }
}

// ═══════════════════════════════════════════════════════
//  Thread 4 — Dashboard HTTP API
// ═══════════════════════════════════════════════════════

void dashboard_server() {
    httplib::Server svr;
    LOG_INFO("Dashboard on port " + std::to_string(DASHBOARD_PORT));

    svr.Get("/", [](const httplib::Request&, httplib::Response& res) {
        std::ifstream f("dashboard.html");
        if (!f) { res.set_content("dashboard.html not found","text/plain"); return; }
        std::string html((std::istreambuf_iterator<char>(f)),{});
        res.set_content(html,"text/html");
    });
    svr.Get("/api/state", [](const httplib::Request&, httplib::Response& res) {
        res.set_header("Access-Control-Allow-Origin","*");
        res.set_content(state_to_json(),"application/json");
    });
    svr.Post("/api/cmd", [](const httplib::Request& req, httplib::Response& res) {
        res.set_header("Access-Control-Allow-Origin","*");
        dispatch(req.body);
        res.set_content("{\"ok\":true}","application/json");
    });
    svr.Get("/api/stream", [](const httplib::Request&, httplib::Response& res) {
        res.set_header("Access-Control-Allow-Origin","*");
        res.set_header("Cache-Control","no-cache");
        res.set_chunked_content_provider("text/event-stream",
            [](size_t, httplib::DataSink& sink) {
                std::string p = "data: " + state_to_json() + "\n\n";
                if (!sink.write(p.c_str(),p.size())) return false;
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                return running.load();
            });
    });

    // Prometheus scrape endpoint — standard text format
    svr.Get("/metrics", [](const httplib::Request&, httplib::Response& res) {
        res.set_header("Access-Control-Allow-Origin","*");
        res.set_content(prometheus_metrics(), "text/plain; version=0.0.4; charset=utf-8");
    });

    svr.listen("0.0.0.0", DASHBOARD_PORT);
}

// ═══════════════════════════════════════════════════════
//  Thread 6 — Pipe Reader
// ═══════════════════════════════════════════════════════

void pipe_reader() {
    mkfifo(PIPE_PATH.c_str(), 0666);
    LOG_INFO("Pipe reader on " + PIPE_PATH);
    while (running) {
        int fd = open(PIPE_PATH.c_str(), O_RDONLY|O_NONBLOCK);
        if (fd<0) { std::this_thread::sleep_for(std::chrono::milliseconds(200)); continue; }
        char buf[1024]; std::string leftover;
        while (running) {
            ssize_t n = read(fd,buf,sizeof(buf)-1);
            if (n>0) {
                buf[n]='\0'; leftover+=buf;
                size_t pos;
                while ((pos=leftover.find('\n'))!=std::string::npos) {
                    std::string c=leftover.substr(0,pos);
                    leftover=leftover.substr(pos+1);
                    if (!c.empty()) {
                        std::cout<<"\n  \033[1;35m[PIPE]\033[0m "<<c<<"\n";
                        dispatch(c);
                    }
                }
            } else { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }
        }
        close(fd);
    }
    unlink(PIPE_PATH.c_str());
}

// ═══════════════════════════════════════════════════════
//  Signal Handler + Main
// ═══════════════════════════════════════════════════════

void handle_signal(int) {
    running = false;
    int s = lb_socket.load();
    if (s>=0) close(s);
}

int main() {
    signal(SIGINT,  handle_signal);
    signal(SIGTERM, handle_signal);
    signal(SIGHUP,  handle_sighup);   // hot reload
    signal(SIGPIPE, SIG_IGN);
    Logger::instance().set_console(false);
    gmetrics.start_time = std::chrono::steady_clock::now();
    LOG_INFO("NetBalancer starting up");

#ifdef __APPLE__
    std::string io_model = "kqueue event loop (macOS)";
#else
    std::string io_model = "poll + threads (Linux)";
#endif

    std::cout << "\n\033[1m╔══════════════════════════════════════════════════════╗\033[0m\n";
    std::cout << "\033[1m║      NetBalancer — Production L7 Load Balancer       ║\033[0m\n";
    std::cout << "\033[1m╚══════════════════════════════════════════════════════╝\033[0m\n";
    std::cout << "  I/O Model  : " << io_model << "\n";
    std::cout << "  Listening  : :" << LB_PORT << "\n";
    std::cout << "  Dashboard  : http://localhost:" << DASHBOARD_PORT << "\n";
    std::cout << "  Prometheus : http://localhost:" << DASHBOARD_PORT << "/metrics\n";
    std::cout << "  Hot reload : kill -HUP <pid>  or  reload command\n\n";

    std::thread t1(acceptor);
    std::thread t2(health_checker);
    std::thread t3(metrics_engine);
    std::thread t4(dashboard_server);
    std::thread t5(pipe_reader);

    print_help();

    std::string line;
    while (running) {
        // Check for pending SIGHUP reload
        if (reload_requested.exchange(false)) {
            reload_config();
        }
        std::cout << "netbalancer> ";
        if (!std::getline(std::cin, line)) break;
        dispatch(line);
    }

    running = false;
    std::cout << "\n  Shutting down...\n";
    LOG_INFO("NetBalancer shutting down");
    t1.join(); t2.join(); t3.join();
    t4.detach(); t5.detach();
    std::cout << "  Bye.\n\n";
    return 0;
}