#include "tests/test_framework.hpp"
#include "gateway/gateway_server.hpp"

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

static gateway::GatewayConfig make_server_config(std::uint16_t port) {
    gateway::GatewayConfig cfg;
    cfg.m_tight.host = "127.0.0.1";
    cfg.m_tight.port = port;
    cfg.m_tight.token = "shared-secret";
    cfg.m_tight.mtu = 1400;
    cfg.m_tight.dead_timeout = std::chrono::seconds(30);
    cfg.m_server.heartbeat_seconds = 60;
    return cfg;
}

TEST_CASE("e2e_gateway_client_hello_ack") {
    std::uint16_t port = 21000;
    auto cfg = make_server_config(port);

    std::atomic<int> ds_count{0};
    std::atomic<bool> got_hello{false};
    cfg.m_tight.id = "gateway-21000";

    gateway::GatewayServer server(cfg);
    server.set_downstream_callback(
        [&](const gateway::GatewayMessage&) { ++ds_count; });
    ASSERT_TRUE(server.start());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    creek::TightConfig client_cfg;
    client_cfg.bind = creek::NetAddress("127.0.0.1", port + 1);
    client_cfg.id = "device-e2e";
    client_cfg.token = "shared-secret";
    client_cfg.role = creek::LinkRole::Leaf;
    client_cfg.mtu = 1400;
    client_cfg.dead_timeout = std::chrono::seconds(30);

    std::atomic<int> client_events{0};
    std::mutex client_mutex;
    std::vector<std::string> client_msgs;

    creek::TightTransport client(client_cfg);
    client.set_peer_callback([&](const creek::PeerEvent&) { ++client_events; });
    client.set_message_callback(
        [&](const std::string&, creek::Bytes p) {
            std::lock_guard<std::mutex> lock(client_mutex);
            client_msgs.push_back(std::string(p.begin(), p.end()));
        });
    ASSERT_TRUE(client.start());

    creek::RemotePeer server_peer;
    server_peer.id = cfg.m_tight.id;
    server_peer.address = creek::NetAddress("127.0.0.1", port);
    ASSERT_TRUE(client.connect(server_peer));

    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    std::string hello_json = "{"
        "\"type\":\"hello\","
        "\"product_id\":\"test-pid\","
        "\"product_key\":\"test-key\","
        "\"product_secret\":\"test-sec\","
        "\"device_name\":\"device-e2e\""
        "}";
    creek::Bytes payload(hello_json.begin(), hello_json.end());
    client.send(cfg.m_tight.id, std::move(payload));

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    {
        std::lock_guard<std::mutex> lock(client_mutex);
        ASSERT_GE(client_msgs.size(), 1u);

        bool found_ack = false;
        for (const auto& m : client_msgs) {
            if (m.find("hello_ack") != std::string::npos)
                found_ack = true;
        }
        ASSERT_TRUE(found_ack);
    }

    client.stop();
    server.stop();
}

TEST_CASE("e2e_gateway_client_ping_pong") {
    std::uint16_t port = 21001;
    auto cfg = make_server_config(port);
    cfg.m_tight.id = "gateway-21001";

    gateway::GatewayServer server(cfg);
    ASSERT_TRUE(server.start());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    creek::TightConfig client_cfg;
    client_cfg.bind = creek::NetAddress("127.0.0.1", port + 1);
    client_cfg.id = "device-ping";
    client_cfg.token = "shared-secret";
    client_cfg.role = creek::LinkRole::Leaf;
    client_cfg.mtu = 1400;
    client_cfg.dead_timeout = std::chrono::seconds(30);

    std::mutex c_mutex;
    std::vector<std::string> c_msgs;

    creek::TightTransport client(client_cfg);
    client.set_message_callback(
        [&](const std::string&, creek::Bytes p) {
            std::lock_guard<std::mutex> lock(c_mutex);
            c_msgs.push_back(std::string(p.begin(), p.end()));
        });
    ASSERT_TRUE(client.start());

    creek::RemotePeer server_peer;
    server_peer.id = cfg.m_tight.id;
    server_peer.address = creek::NetAddress("127.0.0.1", port);
    ASSERT_TRUE(client.connect(server_peer));

    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    std::string hello = "{"
        "\"type\":\"hello\","
        "\"product_id\":\"p\","
        "\"product_key\":\"k\","
        "\"product_secret\":\"s\","
        "\"device_name\":\"device-ping\""
        "}";
    client.send(cfg.m_tight.id, creek::Bytes(hello.begin(), hello.end()));
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::string ping = "{\"type\":\"ping\",\"device_id\":\"device-ping\"}";
    client.send(cfg.m_tight.id, creek::Bytes(ping.begin(), ping.end()));
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    {
        std::lock_guard<std::mutex> lock(c_mutex);
        ASSERT_GE(c_msgs.size(), 1u);
        bool found_pong = false;
        for (const auto& m : c_msgs) {
            if (m.find("pong") != std::string::npos)
                found_pong = true;
        }
        ASSERT_TRUE(found_pong);
    }

    client.stop();
    server.stop();
}

TEST_CASE("e2e_gateway_downstream_forwarding") {
    std::uint16_t port = 21002;
    auto cfg = make_server_config(port);
    cfg.m_tight.id = "gateway-21002";

    std::atomic<int> ds_count{0};
    std::vector<gateway::GatewayMessage> ds_msgs;
    std::mutex ds_mutex;

    gateway::GatewayServer server(cfg);
    server.set_downstream_callback(
        [&](const gateway::GatewayMessage& msg) {
            ++ds_count;
            std::lock_guard<std::mutex> lock(ds_mutex);
            ds_msgs.push_back(msg);
        });
    ASSERT_TRUE(server.start());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    creek::TightConfig client_cfg;
    client_cfg.bind = creek::NetAddress("127.0.0.1", port + 1);
    client_cfg.id = "device-ds";
    client_cfg.token = "shared-secret";
    client_cfg.role = creek::LinkRole::Leaf;
    client_cfg.mtu = 1400;
    client_cfg.dead_timeout = std::chrono::seconds(30);

    creek::TightTransport client(client_cfg);
    ASSERT_TRUE(client.start());

    creek::RemotePeer server_peer;
    server_peer.id = cfg.m_tight.id;
    server_peer.address = creek::NetAddress("127.0.0.1", port);
    ASSERT_TRUE(client.connect(server_peer));

    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    std::string hello = "{"
        "\"type\":\"hello\","
        "\"product_id\":\"pid\","
        "\"product_key\":\"pkey\","
        "\"product_secret\":\"psec\","
        "\"device_name\":\"device-ds\""
        "}";
    client.send(cfg.m_tight.id, creek::Bytes(hello.begin(), hello.end()));
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::string cfg_update = "{\"type\":\"config_update\","
        "\"device_id\":\"device-ds\","
        "\"body\":{\"voice_type\":\"Warm_Girl\"}}";
    client.send(cfg.m_tight.id, creek::Bytes(cfg_update.begin(), cfg_update.end()));
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    ASSERT_GE(ds_count.load(), 1);

    std::string bye = "{\"type\":\"bye\",\"device_id\":\"device-ds\"}";
    client.send(cfg.m_tight.id, creek::Bytes(bye.begin(), bye.end()));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    ASSERT_GE(ds_count.load(), 1);

    client.stop();
    server.stop();
}

TEST_CASE("e2e_gateway_auth_failure") {
    std::uint16_t port = 21003;
    auto cfg = make_server_config(port);
    cfg.m_tight.id = "gateway-21003";

    gateway::GatewayServer server(cfg);
    ASSERT_TRUE(server.start());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    creek::TightConfig client_cfg;
    client_cfg.bind = creek::NetAddress("127.0.0.1", port + 1);
    client_cfg.id = "device-authfail";
    client_cfg.token = "shared-secret";
    client_cfg.role = creek::LinkRole::Leaf;
    client_cfg.mtu = 1400;
    client_cfg.dead_timeout = std::chrono::seconds(30);

    std::mutex cmutex;
    std::vector<std::string> cmsgs;

    creek::TightTransport client(client_cfg);
    client.set_message_callback(
        [&](const std::string&, creek::Bytes p) {
            std::lock_guard<std::mutex> lock(cmutex);
            cmsgs.push_back(std::string(p.begin(), p.end()));
        });
    ASSERT_TRUE(client.start());

    creek::RemotePeer server_peer;
    server_peer.id = cfg.m_tight.id;
    server_peer.address = creek::NetAddress("127.0.0.1", port);
    ASSERT_TRUE(client.connect(server_peer));

    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    std::string bad_hello = "{"
        "\"type\":\"hello\","
        "\"product_id\":\"\","
        "\"product_key\":\"\","
        "\"product_secret\":\"\","
        "\"device_name\":\"\""
        "}";
    client.send(cfg.m_tight.id, creek::Bytes(bad_hello.begin(), bad_hello.end()));
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    {
        std::lock_guard<std::mutex> lock(cmutex);
        ASSERT_GE(cmsgs.size(), 1u);
        bool found_err = false;
        for (const auto& m : cmsgs) {
            if (m.find("hello_err") != std::string::npos)
                found_err = true;
        }
        ASSERT_TRUE(found_err);
    }

    client.stop();
    server.stop();
}
