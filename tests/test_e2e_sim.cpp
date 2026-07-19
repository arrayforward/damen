#include "tests/test_framework.hpp"
#include "tests/sim_device.hpp"
#include "gateway/gateway_server.hpp"

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// SimDevice-based end-to-end verification of every built-in message
// listener (observer-pattern dispatch) against a real GatewayServer over
// real UDP: hello/auth, ping, config update, function-call output, audio
// path, bye and multi-device handling.

static constexpr std::uint16_t kSimBasePort = 21000;

static gateway::GatewayConfig make_config(std::uint16_t port) {
    gateway::GatewayConfig cfg;
    cfg.m_tight.host = "127.0.0.1";
    cfg.m_tight.port = port;
    cfg.m_tight.token = "gateway-shared-secret";
    cfg.m_tight.mtu = 1400;
    cfg.m_tight.heartbeat = std::chrono::seconds(5);
    cfg.m_tight.dead_timeout = std::chrono::seconds(30);
    cfg.m_server.heartbeat_seconds = 60;
    return cfg;
}

static bool wait_until(const std::function<bool()>& pred,
                       std::chrono::milliseconds timeout = std::chrono::seconds(3)) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

struct TestServer {
    gateway::GatewayServer server;
    std::vector<gateway::GatewayMessage> downstream;
    std::mutex ds_mutex;

    explicit TestServer(std::uint16_t port) : server(make_config(port)) {
        server.set_downstream_callback(
            [this](const gateway::GatewayMessage& msg) {
                std::lock_guard<std::mutex> lock(ds_mutex);
                downstream.push_back(msg);
            });
    }

    bool start() { return server.start(); }
    void stop() { server.stop(); }

    bool has_downstream(gateway::GatewayMessageType type,
                        const std::string& device = "") {
        std::lock_guard<std::mutex> lock(ds_mutex);
        for (const auto& m : downstream) {
            if (m.m_type != type) continue;
            if (!device.empty() && m.m_device_id != device) continue;
            return true;
        }
        return false;
    }

    bool device_connected(const std::string& device) {
        auto snapshot = server.board()->device_snapshot();
        auto it = snapshot.find(device);
        return it != snapshot.end() && it->second.m_connected;
    }
};

static std::unique_ptr<gateway::SimDevice>
start_device(const std::string& name, std::uint16_t port) {
    auto dev = std::make_unique<gateway::SimDevice>(name, port);
    if (!dev->start()) return nullptr;
    if (!dev->wait_for_connected(std::chrono::seconds(3))) return nullptr;
    return dev;
}

TEST_CASE("sim_hello_ack_and_downstream") {
    TestServer srv(kSimBasePort);
    ASSERT_TRUE(srv.start());

    auto dev = start_device("sim-hello-1", kSimBasePort);
    ASSERT_NE(dev, nullptr);

    ASSERT_TRUE(dev->send_hello("product-1", "key-1", "secret-1"));
    ASSERT_TRUE(dev->wait_for_json_containing("hello_ack", std::chrono::seconds(3)));
    // HelloListener forwards the onboarding downstream.
    ASSERT_TRUE(wait_until([&] {
        return srv.has_downstream(gateway::GatewayMessageType::kDeviceHello,
                                  "sim-hello-1");
    }));
    ASSERT_TRUE(wait_until([&] { return srv.device_connected("sim-hello-1"); }));

    srv.stop();
}

TEST_CASE("sim_auth_failure") {
    TestServer srv(kSimBasePort + 1);
    ASSERT_TRUE(srv.start());

    // Empty device_name -> authentication fails -> hello_err.
    auto dev = start_device("", kSimBasePort + 1);
    ASSERT_NE(dev, nullptr);
    ASSERT_TRUE(dev->send_hello("product-1", "key-1", "secret-1"));
    ASSERT_TRUE(dev->wait_for_json_containing("hello_err", std::chrono::seconds(3)));

    srv.stop();
}

TEST_CASE("sim_ping_pong") {
    TestServer srv(kSimBasePort + 2);
    ASSERT_TRUE(srv.start());

    auto dev = start_device("sim-ping-1", kSimBasePort + 2);
    ASSERT_NE(dev, nullptr);
    ASSERT_TRUE(dev->send_hello());
    ASSERT_TRUE(dev->wait_for_json_containing("hello_ack", std::chrono::seconds(3)));

    // DeviceListener::on_ping answers with a pong.
    dev->clear_received();
    ASSERT_TRUE(dev->send_ping());
    ASSERT_TRUE(dev->wait_for_json_containing("pong", std::chrono::seconds(3)));

    srv.stop();
}

TEST_CASE("sim_config_update_forwarded") {
    TestServer srv(kSimBasePort + 3);
    ASSERT_TRUE(srv.start());

    auto dev = start_device("sim-config-1", kSimBasePort + 3);
    ASSERT_NE(dev, nullptr);
    ASSERT_TRUE(dev->send_hello());
    ASSERT_TRUE(dev->wait_for_json_containing("hello_ack", std::chrono::seconds(3)));

    // ForwardListener passes config updates downstream.
    ASSERT_TRUE(dev->send_config_update("{\"volume\":80}"));
    ASSERT_TRUE(wait_until([&] {
        return srv.has_downstream(gateway::GatewayMessageType::kDeviceConfigUpdate,
                                  "sim-config-1");
    }));

    srv.stop();
}

TEST_CASE("sim_function_call_output_forwarded") {
    TestServer srv(kSimBasePort + 4);
    ASSERT_TRUE(srv.start());

    auto dev = start_device("sim-func-1", kSimBasePort + 4);
    ASSERT_NE(dev, nullptr);
    ASSERT_TRUE(dev->send_hello());
    ASSERT_TRUE(dev->wait_for_json_containing("hello_ack", std::chrono::seconds(3)));

    // ForwardListener passes function-call outputs downstream.
    ASSERT_TRUE(dev->send_function_call_output("call-42", "{\"result\":\"ok\"}"));
    ASSERT_TRUE(wait_until([&] {
        return srv.has_downstream(
            gateway::GatewayMessageType::kDeviceFunctionCallOutput, "sim-func-1");
    }));

    srv.stop();
}

TEST_CASE("sim_audio_frame_and_boundary_forwarded") {
    TestServer srv(kSimBasePort + 5);
    ASSERT_TRUE(srv.start());

    auto dev = start_device("sim-audio-1", kSimBasePort + 5);
    ASSERT_NE(dev, nullptr);
    ASSERT_TRUE(dev->send_hello());
    ASSERT_TRUE(dev->wait_for_json_containing("hello_ack", std::chrono::seconds(3)));

    // AudioListener: frames (rate-limited, accounted) and boundaries both
    // flow downstream.
    std::vector<std::uint8_t> g711(160, 0xD5);
    ASSERT_TRUE(dev->send_audio_frame(g711));
    ASSERT_TRUE(dev->send_audio_boundary("speaking"));
    ASSERT_TRUE(wait_until([&] {
        return srv.has_downstream(gateway::GatewayMessageType::kDeviceAudioFrame,
                                  "sim-audio-1");
    }));
    ASSERT_TRUE(wait_until([&] {
        return srv.has_downstream(gateway::GatewayMessageType::kDeviceAudioBoundary,
                                  "sim-audio-1");
    }));

    srv.stop();
}

TEST_CASE("sim_bye_closes_session") {
    TestServer srv(kSimBasePort + 6);
    ASSERT_TRUE(srv.start());

    auto dev = start_device("sim-bye-1", kSimBasePort + 6);
    ASSERT_NE(dev, nullptr);
    ASSERT_TRUE(dev->send_hello());
    ASSERT_TRUE(dev->wait_for_json_containing("hello_ack", std::chrono::seconds(3)));
    ASSERT_TRUE(wait_until([&] { return srv.device_connected("sim-bye-1"); }));

    // DeviceListener::on_bye closes the session and forwards downstream.
    ASSERT_TRUE(dev->send_bye());
    ASSERT_TRUE(wait_until([&] {
        return srv.has_downstream(gateway::GatewayMessageType::kDeviceBye,
                                  "sim-bye-1");
    }));
    ASSERT_TRUE(wait_until([&] { return !srv.device_connected("sim-bye-1"); }));

    srv.stop();
}

TEST_CASE("sim_multi_device") {
    TestServer srv(kSimBasePort + 7);
    ASSERT_TRUE(srv.start());

    auto dev1 = start_device("sim-multi-1", kSimBasePort + 7);
    ASSERT_NE(dev1, nullptr);
    // Distinct local port: the default (gateway port + 1) is taken by dev1.
    auto dev2 = std::make_unique<gateway::SimDevice>(
        "sim-multi-2", kSimBasePort + 7, kSimBasePort + 107);
    ASSERT_TRUE(dev2->start());
    ASSERT_TRUE(dev2->wait_for_connected(std::chrono::seconds(3)));

    ASSERT_TRUE(dev1->send_hello());
    ASSERT_TRUE(dev2->send_hello());
    ASSERT_TRUE(dev1->wait_for_json_containing("hello_ack", std::chrono::seconds(3)));
    ASSERT_TRUE(dev2->wait_for_json_containing("hello_ack", std::chrono::seconds(3)));

    ASSERT_TRUE(wait_until([&] {
        return srv.has_downstream(gateway::GatewayMessageType::kDeviceHello,
                                  "sim-multi-1") &&
               srv.has_downstream(gateway::GatewayMessageType::kDeviceHello,
                                  "sim-multi-2");
    }));
    ASSERT_TRUE(wait_until([&] {
        return srv.device_connected("sim-multi-1") &&
               srv.device_connected("sim-multi-2");
    }));

    srv.stop();
}
