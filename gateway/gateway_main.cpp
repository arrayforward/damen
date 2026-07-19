#include "gateway/gateway_config.hpp"
#include "gateway/gateway_server.hpp"
#include "gateway/gateway_metrics_http.hpp"
#include "tight/logger.hpp"

#include <atomic>
#include <csignal>
#include <iostream>
#include <thread>

namespace {

std::atomic<bool> g_running{true};

void signal_handler(int sig) {
    tight::Logger::instance().log(tight::LogLevel::Info,
        "Signal " + std::to_string(sig) + " received, shutting down...");
    g_running.store(false);
}

void setup_signals() {
#ifdef _WIN32
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
#else
    struct sigaction sa{};
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
#endif
}

} // anonymous namespace

int main(int argc, char* argv[]) {
    tight::Logger::instance().set_level(tight::LogLevel::Debug);
    TIGHT_LOG_INFO("=== ConvAI Cloud Gateway v2.0 ===");
    TIGHT_LOG_INFO("Starting with Tight Transport Protocol");

    setup_signals();

    gateway::GatewayConfig config;

    config.m_tight.host = "0.0.0.0";
    config.m_tight.port = 9443;
    config.m_tight.token = "gateway-shared-secret";
    config.m_tight.mtu = 1400;
    config.m_tight.heartbeat = std::chrono::seconds(5);
    config.m_tight.dead_timeout = std::chrono::seconds(30);

    config.m_server.max_connections = 5000;
    config.m_server.heartbeat_seconds = 30;
    config.m_server.max_payload_bytes = 65536;
    config.m_server.session_timeout = std::chrono::minutes(60);

    config.m_ratelimit.connection_cooldown = std::chrono::seconds(10);
    config.m_ratelimit.max_bandwidth_bytes_per_sec = 32 * 1024;

    config.m_downstream.target = "media-service:50051";
    config.m_downstream.connect_timeout = std::chrono::seconds(10);

    config.m_metrics.port = 9090;
    config.m_metrics.path = "/metrics";

    gateway::GatewayServer server(config);

    gateway::MetricsHttpServer metrics_server(config, server.metrics());

    server.set_downstream_callback(
        [](const gateway::GatewayMessage& msg) {
            TIGHT_LOG_DEBUG("Downstream: type=" +
                           std::to_string(static_cast<int>(msg.m_type)) +
                           " device=" + msg.m_device_id +
                           " session=" + msg.m_session_id);
        });

    server.set_downstream_error_callback(
        [](const std::string& session_id, const std::string& error) {
            TIGHT_LOG_ERROR("Downstream error: session=" + session_id +
                            " err=" + error);
        });

    if (!server.start()) {
        TIGHT_LOG_ERROR("Failed to start gateway server");
        return 1;
    }

    metrics_server.start();

    TIGHT_LOG_INFO("Gateway server running. Press Ctrl+C to stop.");

    while (g_running.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    TIGHT_LOG_INFO("Shutting down...");
    metrics_server.stop();
    server.stop();

    TIGHT_LOG_INFO("Gateway server exited cleanly");
    return 0;
}
