#include "tests/test_framework.hpp"

#include "tight/tight.hpp"
#include "gateway/gateway_server.hpp"

#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

TEST_CASE("e2e_step_by_step") {
    std::uint16_t p = 26000;

    printf("=== Step 1: raw tight pair\n");
    {
        tight::TightConfig sc;
        sc.bind = tight::NetAddress("127.0.0.1", p);
        sc.id = "s";
        sc.token = "tok";
        sc.role = tight::LinkRole::Node;
        sc.mtu = 1400;

        tight::TightTransport s(sc);
        std::atomic<int> sev{0};
        s.set_peer_callback([&](const tight::PeerEvent&) { ++sev; });
        ASSERT_TRUE(s.start());

        tight::TightConfig cc;
        cc.bind = tight::NetAddress("127.0.0.1", p + 1);
        cc.id = "c";
        cc.token = "tok";
        cc.role = tight::LinkRole::Leaf;
        cc.mtu = 1400;

        tight::TightTransport cl(cc);
        std::atomic<int> cev{0};
        cl.set_peer_callback([&](const tight::PeerEvent&) { ++cev; });
        ASSERT_TRUE(cl.start());

        tight::RemotePeer rp;
        rp.id = "s";
        rp.address = tight::NetAddress("127.0.0.1", p);
        ASSERT_TRUE(cl.connect(rp));
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        ASSERT_GE(sev.load(), 1);
        ASSERT_GE(cev.load(), 1);
        printf("  Step 1 PASS: srv=%d cli=%d\n", sev.load(), cev.load());
        cl.stop();
        s.stop();
    }

    p = 26002;
    printf("=== Step 2: gateway tight only (no reactor)\n");
    {
        gateway::GatewayConfig gcfg;
        gcfg.m_tight.host = "127.0.0.1";
        gcfg.m_tight.port = p;
        gcfg.m_tight.token = "tok";
        gcfg.m_tight.id = "gw";
        gcfg.m_tight.mtu = 1400;
        gcfg.m_tight.dead_timeout = std::chrono::seconds(30);
        gcfg.m_server.heartbeat_seconds = 60;

        gateway::GatewayServer gw(gcfg);
        ASSERT_TRUE(gw.start());
        printf("  Gateway started on port %u\n", p);

        tight::TightConfig cc;
        cc.bind = tight::NetAddress("127.0.0.1", p + 1);
        cc.id = "cli2";
        cc.token = "tok";
        cc.role = tight::LinkRole::Leaf;
        cc.mtu = 1400;
        cc.dead_timeout = std::chrono::seconds(30);

        tight::TightTransport cl(cc);
        std::atomic<int> cev{0};
        cl.set_peer_callback([&](const tight::PeerEvent& ev) {
            printf("  [CLI] peer ev: state=%d id=%s\n",
                   (int)ev.state, ev.id.c_str());
            ++cev;
        });
        ASSERT_TRUE(cl.start());

        tight::RemotePeer rp;
        rp.id = "gw";
        rp.address = tight::NetAddress("127.0.0.1", p);
        ASSERT_TRUE(cl.connect(rp));
        printf("  Client connected, waiting for handshake...\n");

        for (int i = 0; i < 30; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            if (cev.load() > 0) break;
        }
        ASSERT_GE(cev.load(), 1);
        printf("  Step 2 PASS: cli events=%d\n", cev.load());
        cl.stop();
        gw.stop();
    }

    p = 26004;
    printf("=== Step 3: full hello ack flow\n");
    {
        gateway::GatewayConfig gcfg;
        gcfg.m_tight.host = "127.0.0.1";
        gcfg.m_tight.port = p;
        gcfg.m_tight.token = "tok";
        gcfg.m_tight.id = "gw3";
        gcfg.m_tight.mtu = 1400;
        gcfg.m_tight.dead_timeout = std::chrono::seconds(30);
        gcfg.m_server.heartbeat_seconds = 60;

        gateway::GatewayServer gw(gcfg);
        std::atomic<int> ds{0};
        gw.set_downstream_callback([&](const gateway::GatewayMessage&) { ++ds; });
        ASSERT_TRUE(gw.start());

        tight::TightConfig cc;
        cc.bind = tight::NetAddress("127.0.0.1", p + 1);
        cc.id = "cli3";
        cc.token = "tok";
        cc.role = tight::LinkRole::Leaf;
        cc.mtu = 1400;
        cc.dead_timeout = std::chrono::seconds(30);

        std::atomic<int> cev{0};
        std::vector<std::string> cmsgs;
        std::mutex cmtx;

        tight::TightTransport cl(cc);
        cl.set_peer_callback([&](const tight::PeerEvent&) { ++cev; });
        cl.set_message_callback(
            [&](const std::string&, tight::Bytes p) {
                std::lock_guard<std::mutex> lk(cmtx);
                cmsgs.emplace_back(p.begin(), p.end());
                printf("  [CLI] got: %s\n", cmsgs.back().c_str());
            });
        ASSERT_TRUE(cl.start());

        tight::RemotePeer rp;
        rp.id = "gw3";
        rp.address = tight::NetAddress("127.0.0.1", p);
        ASSERT_TRUE(cl.connect(rp));

        for (int i = 0; i < 30; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            if (cev.load() > 0) break;
        }
        ASSERT_GE(cev.load(), 1);

        std::string hello = "{\"type\":\"hello\",\"product_id\":\"p\","
            "\"product_key\":\"k\",\"product_secret\":\"s\","
            "\"device_name\":\"cli3\"}";
        cl.send("gw3", tight::Bytes(hello.begin(), hello.end()));

        for (int i = 0; i < 30; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            {
                std::lock_guard<std::mutex> lk(cmtx);
                if (!cmsgs.empty()) break;
            }
        }

        {
            std::lock_guard<std::mutex> lk(cmtx);
            ASSERT_GE(cmsgs.size(), 1u);
            bool ok = false;
            for (auto& s : cmsgs) {
                if (s.find("hello_ack") != std::string::npos) ok = true;
            }
            ASSERT_TRUE(ok);
        }

        printf("  Step 3 PASS: msgs=%zu\n", cmsgs.size());
        cl.stop();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        gw.stop();
    }

    printf("All steps PASSED!\n");
}
