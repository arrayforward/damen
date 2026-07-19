#include "tests/test_framework.hpp"
#include "tests/sim_device.hpp"
#include "gateway/gateway_server.hpp"

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

static constexpr std::uint16_t kBasePort = 19000;

static gateway::GatewayConfig make_server_config(std::uint16_t port) {
    gateway::GatewayConfig cfg;
    cfg.m_tight.host = "127.0.0.1";
    cfg.m_tight.port = port;
    cfg.m_tight.token = "gateway-shared-secret";
    cfg.m_tight.mtu = 1400;
    cfg.m_tight.heartbeat = std::chrono::seconds(5);
    cfg.m_tight.dead_timeout = std::chrono::seconds(30);
    cfg.m_server.max_connections = 100;
    cfg.m_server.heartbeat_seconds = 60;
    return cfg;
}

static std::shared_ptr<gateway::GatewayServer>
start_server(std::uint16_t port) {
    auto svr = std::make_shared<gateway::GatewayServer>(make_server_config(port));
    if (!svr->start()) return nullptr;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    return svr;
}

TEST_CASE("e2e_server_start_and_stop") {
    auto svr = start_server(kBasePort);
    ASSERT_NE(svr, nullptr);
    svr->stop();
}

TEST_CASE("e2e_device_hello_and_ack") {
    auto svr = start_server(kBasePort + 1);
    ASSERT_NE(svr, nullptr);

    gateway::SimDevice dev("device-e2e-1", kBasePort + 1);
    ASSERT_TRUE(dev.start());

    ASSERT_TRUE(dev.wait_for_connected(std::chrono::seconds(3)));

    dev.send_hello("product-1", "key-1", "secret-1");
    ASSERT_TRUE(dev.wait_for_json_containing("hello_ack", std::chrono::seconds(3)));

    svr->stop();
}

TEST_CASE("e2e_device_ping_pong") {
    auto svr = start_server(kBasePort + 2);
    ASSERT_NE(svr, nullptr);

    gateway::SimDevice dev("device-e2e-2", kBasePort + 2);
    ASSERT_TRUE(dev.start());
    ASSERT_TRUE(dev.wait_for_connected(std::chrono::seconds(3)));

    dev.send_hello("product-1", "key-1", "secret-1");
    ASSERT_TRUE(dev.wait_for_json_containing("hello_ack", std::chrono::seconds(3)));

    dev.clear_received();
    dev.send_ping();
    ASSERT_TRUE(dev.wait_for_json_containing("pong", std::chrono::seconds(3)));

    svr->stop();
}

TEST_CASE("e2e_device_auth_failure") {
    auto svr = start_server(kBasePort + 3);
    ASSERT_NE(svr, nullptr);

    gateway::SimDevice dev("", kBasePort + 3);
    ASSERT_TRUE(dev.start());
    ASSERT_TRUE(dev.wait_for_connected(std::chrono::seconds(3)));

    dev.send_hello("", "", "");
    ASSERT_TRUE(dev.wait_for_json_containing("hello_err", std::chrono::seconds(3)));
    ASSERT_TRUE(dev.has_received_containing("AUTH_FAILED"));

    svr->stop();
}

TEST_CASE("e2e_downstream_callback_receives_hello") {
    auto svr = start_server(kBasePort + 4);
    ASSERT_NE(svr, nullptr);

    std::atomic<int> downstream_count{0};
    gateway::GatewayMessage last_ds_msg;
    svr->set_downstream_callback(
        [&](const gateway::GatewayMessage& msg) {
            ++downstream_count;
            last_ds_msg = msg;
        });

    gateway::SimDevice dev("device-e2e-4", kBasePort + 4);
    ASSERT_TRUE(dev.start());
    ASSERT_TRUE(dev.wait_for_connected(std::chrono::seconds(3)));

    dev.send_hello("product-4", "key-4", "secret-4");
    ASSERT_TRUE(dev.wait_for_json_containing("hello_ack", std::chrono::seconds(3)));

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ASSERT_GE(downstream_count.load(), 1);

    svr->stop();
}

TEST_CASE("e2e_downstream_receives_config_update") {
    auto svr = start_server(kBasePort + 5);
    ASSERT_NE(svr, nullptr);

    std::atomic<int> ds_count{0};
    svr->set_downstream_callback(
        [&](const gateway::GatewayMessage&) { ++ds_count; });

    gateway::SimDevice dev("device-e2e-5", kBasePort + 5);
    ASSERT_TRUE(dev.start());
    ASSERT_TRUE(dev.wait_for_connected(std::chrono::seconds(3)));
    dev.send_hello("product-5", "key-5", "secret-5");
    ASSERT_TRUE(dev.wait_for_json_containing("hello_ack", std::chrono::seconds(3)));

    dev.send_config_update("{\"voice_type\":\"Warm_Girl\"}");
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ASSERT_GE(ds_count.load(), 2);

    svr->stop();
}

TEST_CASE("e2e_downstream_receives_function_call_output") {
    auto svr = start_server(kBasePort + 6);
    ASSERT_NE(svr, nullptr);

    std::atomic<int> ds_count{0};
    svr->set_downstream_callback(
        [&](const gateway::GatewayMessage&) { ++ds_count; });

    gateway::SimDevice dev("device-e2e-6", kBasePort + 6);
    ASSERT_TRUE(dev.start());
    ASSERT_TRUE(dev.wait_for_connected(std::chrono::seconds(3)));
    dev.send_hello("product-6", "key-6", "secret-6");
    ASSERT_TRUE(dev.wait_for_json_containing("hello_ack", std::chrono::seconds(3)));

    dev.send_function_call_output("call_1", "{\"result\":\"ok\"}");
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ASSERT_GE(ds_count.load(), 2);

    svr->stop();
}

TEST_CASE("e2e_downstream_receives_audio_frame") {
    auto svr = start_server(kBasePort + 7);
    ASSERT_NE(svr, nullptr);

    std::atomic<int> ds_count{0};
    svr->set_downstream_callback(
        [&](const gateway::GatewayMessage&) { ++ds_count; });

    gateway::SimDevice dev("device-e2e-7", kBasePort + 7);
    ASSERT_TRUE(dev.start());
    ASSERT_TRUE(dev.wait_for_connected(std::chrono::seconds(3)));
    dev.send_hello("product-7", "key-7", "secret-7");
    ASSERT_TRUE(dev.wait_for_json_containing("hello_ack", std::chrono::seconds(3)));

    std::vector<std::uint8_t> audio(160, 0x7F);
    for (int i = 0; i < 10; ++i) dev.send_audio_frame(audio);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ASSERT_GE(ds_count.load(), 2);

    svr->stop();
}

TEST_CASE("e2e_downstream_receives_audio_boundary") {
    auto svr = start_server(kBasePort + 8);
    ASSERT_NE(svr, nullptr);

    std::atomic<int> ds_count{0};
    svr->set_downstream_callback(
        [&](const gateway::GatewayMessage&) { ++ds_count; });

    gateway::SimDevice dev("device-e2e-8", kBasePort + 8);
    ASSERT_TRUE(dev.start());
    ASSERT_TRUE(dev.wait_for_connected(std::chrono::seconds(3)));
    dev.send_hello("product-8", "key-8", "secret-8");
    ASSERT_TRUE(dev.wait_for_json_containing("hello_ack", std::chrono::seconds(3)));

    dev.send_audio_boundary("speech_start");
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ASSERT_GE(ds_count.load(), 2);

    svr->stop();
}

TEST_CASE("e2e_downstream_receives_bye") {
    auto svr = start_server(kBasePort + 9);
    ASSERT_NE(svr, nullptr);

    std::atomic<int> ds_count{0};
    svr->set_downstream_callback(
        [&](const gateway::GatewayMessage&) { ++ds_count; });

    gateway::SimDevice dev("device-e2e-9", kBasePort + 9);
    ASSERT_TRUE(dev.start());
    ASSERT_TRUE(dev.wait_for_connected(std::chrono::seconds(3)));
    dev.send_hello("product-9", "key-9", "secret-9");
    ASSERT_TRUE(dev.wait_for_json_containing("hello_ack", std::chrono::seconds(3)));

    dev.send_bye();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ASSERT_GE(ds_count.load(), 2);

    svr->stop();
}

TEST_CASE("e2e_connection_lost_then_closed") {
    auto svr = start_server(kBasePort + 10);
    ASSERT_NE(svr, nullptr);

    gateway::SimDevice dev("device-e2e-10", kBasePort + 10);
    ASSERT_TRUE(dev.start());
    ASSERT_TRUE(dev.wait_for_connected(std::chrono::seconds(3)));

    dev.send_hello("product-10", "key-10", "secret-10");
    ASSERT_TRUE(dev.wait_for_json_containing("hello_ack", std::chrono::seconds(3)));

    dev.stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    gateway::SimDevice dev2("device-e2e-10", kBasePort + 10);
    ASSERT_TRUE(dev2.start());
    ASSERT_TRUE(dev2.wait_for_connected(std::chrono::seconds(3)));

    dev2.send_hello("product-10", "key-10", "secret-10");
    ASSERT_TRUE(dev2.wait_for_json_containing("hello_ack", std::chrono::seconds(3)));

    svr->stop();
}

TEST_CASE("e2e_multiple_devices_concurrent") {
    auto svr = start_server(kBasePort + 11);
    ASSERT_NE(svr, nullptr);

    constexpr int kNumDevices = 5;
    std::vector<std::unique_ptr<gateway::SimDevice>> devices;
    for (int i = 0; i < kNumDevices; ++i) {
        auto dev = std::make_unique<gateway::SimDevice>(
            "device-multi-" + std::to_string(i),
            kBasePort + 11);
        ASSERT_TRUE(dev->start());
        ASSERT_TRUE(dev->wait_for_connected(std::chrono::seconds(3)));
        dev->send_hello("product", "key", "secret");
        devices.push_back(std::move(dev));
    }

    for (auto& dev : devices) {
        ASSERT_TRUE(dev->wait_for_json_containing("hello_ack",
            std::chrono::seconds(5)));
    }

    svr->stop();
}

TEST_CASE("e2e_full_conversation_flow") {
    auto svr = start_server(kBasePort + 12);
    ASSERT_NE(svr, nullptr);

    std::atomic<int> ds_count{0};
    std::vector<gateway::GatewayMessage> ds_msgs;
    svr->set_downstream_callback(
        [&](const gateway::GatewayMessage& msg) {
            ++ds_count;
            ds_msgs.push_back(msg);
        });

    gateway::SimDevice dev("device-flow", kBasePort + 12);
    ASSERT_TRUE(dev.start());
    ASSERT_TRUE(dev.wait_for_connected(std::chrono::seconds(3)));

    dev.send_hello("product-flow", "key-flow", "secret-flow");
    ASSERT_TRUE(dev.wait_for_json_containing("hello_ack", std::chrono::seconds(3)));
    ASSERT_TRUE(dev.has_received_containing("\"type\":\"event\""));
    ASSERT_TRUE(dev.has_received_containing("connected"));
    ASSERT_TRUE(dev.has_received_containing("\"type\":\"status\""));
    ASSERT_TRUE(dev.has_received_containing("idle"));

    dev.clear_received();
    dev.send_ping();
    ASSERT_TRUE(dev.wait_for_json_containing("pong", std::chrono::seconds(3)));

    dev.send_config_update("{\"voice_type\":\"Warm_Girl\"}");
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    std::vector<std::uint8_t> audio(160, 0x80);
    for (int i = 0; i < 5; ++i) dev.send_audio_frame(audio);

    dev.send_audio_boundary("speech_end");
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    dev.send_function_call_output("call_flow", "{\"result\":\"success\"}");
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    dev.send_bye();
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    ASSERT_GE(ds_count.load(), 6);

    svr->stop();
}

TEST_CASE("e2e_rate_limit_connection") {
    gateway::GatewayConfig cfg = make_server_config(kBasePort + 13 + 100);
    cfg.m_ratelimit.connection_cooldown = std::chrono::seconds(10);
    auto svr = std::make_shared<gateway::GatewayServer>(cfg);
    ASSERT_TRUE(svr->start());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    gateway::SimDevice dev("device-rl", kBasePort + 13 + 100);
    ASSERT_TRUE(dev.start());
    ASSERT_TRUE(dev.wait_for_connected(std::chrono::seconds(3)));

    dev.send_hello("product-rl", "key-rl", "secret-rl");
    ASSERT_TRUE(dev.wait_for_json_containing("hello_ack", std::chrono::seconds(3)));

    dev.clear_received();
    dev.send_hello("product-rl", "key-rl", "secret-rl");
    ASSERT_TRUE(dev.wait_for_json_containing("hello_err", std::chrono::seconds(3)));
    ASSERT_TRUE(dev.has_received_containing("RATE_LIMITED"));

    svr->stop();
}

TEST_CASE("e2e_config_update_forwarded") {
    auto svr = start_server(kBasePort + 14 + 100);
    ASSERT_NE(svr, nullptr);

    gateway::GatewayMessage last_cfg;
    svr->set_downstream_callback(
        [&](const gateway::GatewayMessage& msg) {
            if (msg.m_type == gateway::GatewayMessageType::kDeviceConfigUpdate)
                last_cfg = msg;
        });

    gateway::SimDevice dev("device-cfg", kBasePort + 14 + 100);
    ASSERT_TRUE(dev.start());
    ASSERT_TRUE(dev.wait_for_connected(std::chrono::seconds(3)));
    dev.send_hello("product-cfg", "key-cfg", "secret-cfg");
    ASSERT_TRUE(dev.wait_for_json_containing("hello_ack", std::chrono::seconds(3)));

    std::string cfg = "{\"llm_config\":{\"system_messages\":[\"base\"]},"
                       "\"tts_config\":{\"voice_type\":\"Warm_Girl\"}}";
    dev.send_config_update(cfg);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    ASSERT_EQ(static_cast<int>(last_cfg.m_type),
              static_cast<int>(gateway::GatewayMessageType::kDeviceConfigUpdate));

    svr->stop();
}
