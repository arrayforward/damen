#pragma once

#include <chrono>
#include <cstdint>
#include <string>

namespace gateway {

struct GatewayConfig {
    struct {
        std::string host{"0.0.0.0"};
        std::uint16_t port{9443};
        std::string id;
        std::string token{"gateway-token"};
        std::size_t mtu{1200};
        std::chrono::milliseconds heartbeat{std::chrono::seconds(5)};
        std::chrono::milliseconds dead_timeout{std::chrono::seconds(30)};
    } m_tight;

    struct {
        std::string target{"media-service:50051"};
        std::chrono::milliseconds connect_timeout{std::chrono::seconds(10)};
        std::chrono::milliseconds reconnect_delay{std::chrono::seconds(5)};
    } m_downstream;

    struct {
        std::string url{"redis://redis:6379"};
        std::chrono::milliseconds timeout{std::chrono::milliseconds(200)};
    } m_redis;

    struct {
        std::uint32_t max_connections{5000};
        std::uint32_t heartbeat_seconds{60};
        std::uint64_t max_payload_bytes{65536};
        std::chrono::seconds session_timeout{std::chrono::minutes(60)};
    } m_server;

    struct {
        std::chrono::seconds connection_cooldown{std::chrono::seconds(10)};
        std::uint64_t max_bandwidth_bytes_per_sec{32 * 1024};
        std::chrono::minutes max_audio_per_day{60};
        std::uint64_t max_llm_tokens_per_day{100000};
    } m_ratelimit;

    struct {
        std::uint16_t port{9090};
        std::string path{"/metrics"};
    } m_metrics;

    std::string m_log_level{"info"};
};

} // namespace gateway
