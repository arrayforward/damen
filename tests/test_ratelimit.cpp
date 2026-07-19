#include "tests/test_framework.hpp"
#include "gateway/gateway_ratelimit.hpp"

#include <chrono>
#include <thread>

TEST_CASE("ratelimit_default_allowed") {
    gateway::RateLimiter rl;
    auto result = rl.check("dev-01", "nonexistent");
    ASSERT_TRUE(result.m_allowed);
}

TEST_CASE("ratelimit_connection_allow_first") {
    gateway::RateLimiter rl;
    auto result = rl.check("dev-01", "connection");
    ASSERT_TRUE(result.m_allowed);
}

TEST_CASE("ratelimit_connection_deny_second") {
    gateway::RateLimiter rl;
    auto r1 = rl.check("dev-01", "connection");
    ASSERT_TRUE(r1.m_allowed);
    auto r2 = rl.check("dev-01", "connection");
    ASSERT_FALSE(r2.m_allowed);
    ASSERT_EQ(r2.m_error_code, "RATE_LIMITED");
}

TEST_CASE("ratelimit_bandwidth_large_cost_denied") {
    gateway::RateLimiter rl;
    rl.configure("bw_test", {100.0, 100.0, std::chrono::seconds(1)});
    auto r1 = rl.check("dev-01", "bw_test", 50);
    ASSERT_TRUE(r1.m_allowed);
    auto r2 = rl.check("dev-01", "bw_test", 60);
    ASSERT_FALSE(r2.m_allowed);
}

TEST_CASE("ratelimit_different_devices_independent") {
    gateway::RateLimiter rl;
    auto r1 = rl.check("dev-A", "connection");
    ASSERT_TRUE(r1.m_allowed);
    auto r2 = rl.check("dev-B", "connection");
    ASSERT_TRUE(r2.m_allowed);
}

TEST_CASE("ratelimit_different_dimensions_independent") {
    gateway::RateLimiter rl;
    rl.configure("dim_a", {100.0, 100.0, std::chrono::seconds(1)});
    rl.configure("dim_b", {100.0, 100.0, std::chrono::seconds(1)});
    auto r1 = rl.check("dev-01", "dim_a", 100);
    ASSERT_TRUE(r1.m_allowed);
    auto r2 = rl.check("dev-01", "dim_b", 100);
    ASSERT_TRUE(r2.m_allowed);
}

TEST_CASE("ratelimit_refill_over_time") {
    gateway::RateLimiter rl;
    rl.configure("fast_refill", {100.0, 100.0, std::chrono::milliseconds(100)});
    auto r1 = rl.check("dev-01", "fast_refill", 100);
    ASSERT_TRUE(r1.m_allowed);
    auto r2 = rl.check("dev-01", "fast_refill", 1);
    ASSERT_FALSE(r2.m_allowed);
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    auto r3 = rl.check("dev-01", "fast_refill", 10);
    ASSERT_TRUE(r3.m_allowed);
}

TEST_CASE("ratelimit_configure_overrides") {
    gateway::RateLimiter rl;
    auto r1 = rl.check("dev-01", "connection");
    ASSERT_TRUE(r1.m_allowed);
    auto r2 = rl.check("dev-01", "connection");
    ASSERT_FALSE(r2.m_allowed);
    rl.configure("connection", {100.0, 100.0, std::chrono::seconds(1)});
    auto r3 = rl.check("dev-02", "connection");
    ASSERT_TRUE(r3.m_allowed);
}

TEST_CASE("ratelimit_remaining_tokens") {
    gateway::RateLimiter rl;
    rl.configure("exact", {10.0, 100.0, std::chrono::seconds(1)});
    auto r1 = rl.check("dev-01", "exact", 30);
    ASSERT_TRUE(r1.m_allowed);
    ASSERT_GE(r1.m_remaining, 65u);
    ASSERT_LE(r1.m_remaining, 75u);
}
