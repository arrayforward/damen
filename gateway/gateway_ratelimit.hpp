#pragma once

#include "gateway/gateway_types.hpp"

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

namespace gateway {

class RateLimiter {
public:
    struct BucketConfig {
        double       m_rate{};
        double       m_capacity{};
        std::chrono::milliseconds m_window{1000};
    };

    RateLimiter() {
        m_buckets["connection"]  = {1.0 / 10.0, 1.0, std::chrono::seconds(10)};
        m_buckets["bandwidth"]   = {32 * 1024.0, 32 * 1024.0, std::chrono::seconds(1)};
        m_buckets["audio_day"]   = {60.0 / 86400.0, 60.0, std::chrono::hours(24)};
        m_buckets["tokens_day"]  = {100000.0 / 86400.0, 100000.0, std::chrono::hours(24)};
    }

    void configure(const std::string& dimension, BucketConfig config) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_buckets[dimension] = config;
    }

    RateLimitResult check(const std::string& device_id,
                          const std::string& dimension,
                          std::uint64_t cost = 1) {
        RateLimitResult result;
        result.m_dimension = dimension;
        std::lock_guard<std::mutex> lock(m_mutex);
        auto bucket_it = m_buckets.find(dimension);
        if (bucket_it == m_buckets.end()) {
            result.m_allowed = true;
            return result;
        }
        auto& cfg = bucket_it->second;
        std::string key = device_id + ":" + dimension;
        auto& tb = m_tokens[key];
        auto now = std::chrono::steady_clock::now();
        if (tb.m_last_refill.time_since_epoch().count() == 0) {
            tb.m_tokens = cfg.m_capacity;
            tb.m_last_refill = now;
        }
        auto elapsed = std::chrono::duration<double>(
            now - tb.m_last_refill).count();
        tb.m_tokens += elapsed * cfg.m_rate;
        if (tb.m_tokens > cfg.m_capacity) tb.m_tokens = cfg.m_capacity;
        tb.m_last_refill = now;
        double dcost = static_cast<double>(cost);
        if (tb.m_tokens < dcost) {
            result.m_allowed = false;
            result.m_error_code = "RATE_LIMITED";
            result.m_remaining = static_cast<std::uint64_t>(tb.m_tokens);
            return result;
        }
        tb.m_tokens -= dcost;
        result.m_remaining = static_cast<std::uint64_t>(tb.m_tokens);
        return result;
    }

private:
    struct TokenBucket {
        double                            m_tokens{};
        std::chrono::steady_clock::time_point m_last_refill;
    };

    std::mutex m_mutex;
    std::unordered_map<std::string, BucketConfig> m_buckets;
    std::unordered_map<std::string, TokenBucket> m_tokens;
};

} // namespace gateway
