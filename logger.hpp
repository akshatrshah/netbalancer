#pragma once
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <chrono>
#include <iomanip>
#include <mutex>

// ─────────────────────────────────────────────
//  Lightweight thread-safe logger
//  Writes to both stdout (optional) and ospf.log
// ─────────────────────────────────────────────

enum class LogLevel { INFO, WARN, ERROR, BENCH };

class Logger {
public:
    static Logger& instance() {
        static Logger l;
        return l;
    }

    void set_console(bool on) { console_ = on; }

    void log(LogLevel lvl, const std::string& msg) {
        std::lock_guard<std::mutex> lock(mu_);
        std::string entry = timestamp() + " [" + level_str(lvl) + "] " + msg;
        file_ << entry << "\n";
        file_.flush();
        if (console_) std::cout << entry << "\n";
    }

    // Convenience wrappers
    void info (const std::string& m) { log(LogLevel::INFO,  m); }
    void warn (const std::string& m) { log(LogLevel::WARN,  m); }
    void error(const std::string& m) { log(LogLevel::ERROR, m); }
    void bench(const std::string& m) { log(LogLevel::BENCH, m); }

private:
    Logger() : console_(false) {
        file_.open("ospf.log", std::ios::app);
        file_ << "\n══ Session started " << timestamp() << " ══\n";
    }

    std::string timestamp() {
        auto now = std::chrono::system_clock::now();
        auto t   = std::chrono::system_clock::to_time_t(now);
        auto ms  = std::chrono::duration_cast<std::chrono::milliseconds>(
                       now.time_since_epoch()) % 1000;
        std::ostringstream ss;
        ss << std::put_time(std::localtime(&t), "%H:%M:%S")
           << "." << std::setw(3) << std::setfill('0') << ms.count();
        return ss.str();
    }

    std::string level_str(LogLevel l) {
        switch (l) {
            case LogLevel::INFO:  return "INFO ";
            case LogLevel::WARN:  return "WARN ";
            case LogLevel::ERROR: return "ERROR";
            case LogLevel::BENCH: return "BENCH";
        }
        return "?????";
    }

    std::ofstream file_;
    std::mutex    mu_;
    bool          console_;
};

#define LOG_INFO(m)  Logger::instance().info(m)
#define LOG_WARN(m)  Logger::instance().warn(m)
#define LOG_ERROR(m) Logger::instance().error(m)
#define LOG_BENCH(m) Logger::instance().bench(m)