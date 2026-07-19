#include "tests/test_framework.hpp"
#include "gateway/gateway_server.hpp"

#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

TEST_CASE("e2e_full_flow") {
    std::uint16_t port = 22000;

    gateway::GatewayConfig cfg;
    cfg.m_tight.host = "127.0.0.1";
    cfg.m_tight.port = port;
    cfg.m_tight.token = "shared-secret";
    cfg.m_tight.id = "gateway-22000";
    cfg.m_tight.mtu = 1400;
    cfg.m_tight.dead_timeout = std::chrono::seconds(30);
    cfg.m_server.heartbeat_seconds = 60;

    std::atomic<int> ds_count{0};
    std::vector<std::string> recv;
    std::mutex recv_mtx;

    gateway::GatewayServer server(cfg);
    server.set_downstream_callback([&](const gateway::GatewayMessage&) {
        ++ds_count;
    });
    ASSERT_TRUE(server.start());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    tight::TightConfig cc;
    cc.bind = tight::NetAddress("127.0.0.1", port + 1);
    cc.id = "dev-full";
    cc.token = "shared-secret";
    cc.role = tight::LinkRole::Leaf;
    cc.mtu = 1400;
    cc.dead_timeout = std::chrono::seconds(30);

    tight::TightTransport client(cc);
    client.set_message_callback(
        [&](const std::string&, tight::Bytes p) {
            std::lock_guard<std::mutex> lk(recv_mtx);
            recv.emplace_back(p.begin(), p.end());
        });
    ASSERT_TRUE(client.start());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    tight::RemotePeer rp;
    rp.id = "gateway-22000";
    rp.address = tight::NetAddress("127.0.0.1", port);
    ASSERT_TRUE(client.connect(rp));

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::string hello = "{\"type\":\"hello\",\"product_id\":\"p\","
        "\"product_key\":\"k\",\"product_secret\":\"s\","
        "\"device_name\":\"dev-full\"}";
    auto payload = tight::Bytes(hello.begin(), hello.end());
    ASSERT_TRUE(client.send("gateway-22000", std::move(payload)));

    for (int i = 0; i < 20; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        {
            std::lock_guard<std::mutex> lk(recv_mtx);
            if (!recv.empty()) break;
        }
    }

    {
        std::lock_guard<std::mutex> lk(recv_mtx);
        ASSERT_GE(recv.size(), 1u);
        bool ok = false;
        for (auto& s : recv) if (s.find("hello_ack") != std::string::npos) ok = true;
        ASSERT_TRUE(ok);
    }

    recv.clear();

    std::string ping = "{\"type\":\"ping\",\"device_id\":\"dev-full\"}";
    client.send("gateway-22000", tight::Bytes(ping.begin(), ping.end()));

    for (int i = 0; i < 20; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        {
            std::lock_guard<std::mutex> lk(recv_mtx);
            if (!recv.empty()) break;
        }
    }

    {
        std::lock_guard<std::mutex> lk(recv_mtx);
        bool ok = false;
        for (auto& s : recv) if (s.find("pong") != std::string::npos) ok = true;
        ASSERT_TRUE(ok);
    }

    ASSERT_GE(ds_count.load(), 1);

    client.stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    server.stop();
}
