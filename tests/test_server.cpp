#include "tests/test_framework.hpp"
#include "gateway/gateway_server.hpp"

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

TEST_CASE("server_construction_default_state") {
    gateway::GatewayConfig cfg;
    cfg.m_tight.port = 18000;

    gateway::GatewayServer server(cfg);
    ASSERT_NE(server.board(), nullptr);
    ASSERT_NE(server.metrics(), nullptr);
    ASSERT_NE(server.reactor(), nullptr);
    ASSERT_NE(server.ratelimiter(), nullptr);
}

TEST_CASE("server_downstream_callback") {
    gateway::GatewayConfig cfg;
    cfg.m_tight.port = 18001;
    gateway::GatewayServer server(cfg);

    std::vector<gateway::GatewayMessage> received;
    server.set_downstream_callback(
        [&](const gateway::GatewayMessage& msg) {
            received.push_back(msg);
        });
    ASSERT_EQ(received.size(), 0u);
}

TEST_CASE("server_downstream_error_callback") {
    gateway::GatewayConfig cfg;
    cfg.m_tight.port = 18002;
    gateway::GatewayServer server(cfg);

    std::string last_sid, last_err;
    server.set_downstream_error_callback(
        [&](const std::string& sid, const std::string& err) {
            last_sid = sid;
            last_err = err;
        });
    ASSERT_EQ(last_sid, "");
    ASSERT_EQ(last_err, "");
}

TEST_CASE("server_start_stop") {
    gateway::GatewayConfig cfg;
    cfg.m_tight.port = 18003;
    cfg.m_tight.token = "test-secret";
    cfg.m_tight.mtu = 1200;
    gateway::GatewayServer server(cfg);

    ASSERT_TRUE(server.start());

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_NE(server.board(), nullptr);

    server.stop();
}

TEST_CASE("server_double_start") {
    gateway::GatewayConfig cfg;
    cfg.m_tight.port = 18004;
    gateway::GatewayServer server(cfg);

    ASSERT_TRUE(server.start());
    ASSERT_TRUE(server.start());
    server.stop();
}

TEST_CASE("server_double_stop_no_crash") {
    gateway::GatewayConfig cfg;
    cfg.m_tight.port = 18005;
    gateway::GatewayServer server(cfg);

    ASSERT_TRUE(server.start());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    server.stop();
    server.stop();
}

TEST_CASE("server_start_binds_port") {
    gateway::GatewayConfig cfg;
    cfg.m_tight.port = 18006;
    cfg.m_tight.token = "test";
    gateway::GatewayServer server(cfg);

    ASSERT_TRUE(server.start());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    server.stop();

    gateway::GatewayServer server2(cfg);
    ASSERT_TRUE(server2.start());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    server2.stop();
}

TEST_CASE("server_config_defaults") {
    gateway::GatewayConfig cfg;
    ASSERT_EQ(cfg.m_tight.port, 9443u);
    ASSERT_EQ(cfg.m_tight.host, "0.0.0.0");
    ASSERT_EQ(cfg.m_server.max_connections, 5000u);
    ASSERT_EQ(cfg.m_server.max_payload_bytes, 65536u);
    ASSERT_EQ(cfg.m_metrics.port, 9090u);
}

TEST_CASE("server_message_listener_registration") {
    gateway::GatewayConfig cfg;
    cfg.m_tight.port = 18007;
    gateway::GatewayServer server(cfg);

    // Custom listeners plug into the dispatch without modifying the server;
    // multiple listeners per type are supported.
    int calls = 0;
    ASSERT_NOTHROW(
        server.add_message_listener(gateway::GatewayMessageType::kDevicePing,
            [&](const gateway::GatewayMessage&) { ++calls; }));
    ASSERT_NOTHROW(
        server.add_message_listener(gateway::GatewayMessageType::kDevicePing,
            [&](const gateway::GatewayMessage&) {}));
    ASSERT_EQ(calls, 0);
}

TEST_CASE("server_separate_instances_independent") {
    gateway::GatewayConfig cfg1;
    cfg1.m_tight.port = 18100;
    gateway::GatewayServer server1(cfg1);

    gateway::GatewayConfig cfg2;
    cfg2.m_tight.port = 18101;
    gateway::GatewayServer server2(cfg2);

    ASSERT_NE(server1.board().get(), server2.board().get());
    ASSERT_NE(server1.metrics().get(), server2.metrics().get());
}
