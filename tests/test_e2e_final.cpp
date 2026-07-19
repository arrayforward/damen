#include "tests/test_framework.hpp"
#include "gateway/gateway_server.hpp"

#include <chrono>
#include <cstdio>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

static gateway::GatewayConfig mk_cfg(std::uint16_t port) {
    gateway::GatewayConfig c;
    c.m_tight.host = "127.0.0.1";
    c.m_tight.port = port;
    c.m_tight.token = "tok";
    c.m_tight.id = "gw";
    c.m_tight.mtu = 1400;
    c.m_tight.dead_timeout = std::chrono::seconds(30);
    c.m_server.heartbeat_seconds = 60;
    return c;
}

static void wait_msgs(std::vector<std::string>& cmsgs, std::mutex& mtx,
                      int max_loops = 15) {
    for (int i = 0; i < max_loops; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        {
            std::lock_guard<std::mutex> lk(mtx);
            if (!cmsgs.empty()) return;
        }
    }
}

TEST_CASE("e2e_hello_ack") {
    auto cfg = mk_cfg(27000);
    cfg.m_tight.id = "gw-27000";
    gateway::GatewayServer gw(cfg);
    ASSERT_TRUE(gw.start());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    creek::TightConfig cc;
    cc.bind = creek::NetAddress("127.0.0.1", 27001);
    cc.id = "dev";
    cc.token = "tok";
    cc.role = creek::LinkRole::Leaf;
    cc.mtu = 1400;
    cc.dead_timeout = std::chrono::seconds(30);

    std::vector<std::string> msgs;
    std::mutex mtx;
    std::atomic<int> ev{0};
    creek::TightTransport cli(cc);
    cli.set_peer_callback([&](const creek::PeerEvent&) { ++ev; });
    cli.set_message_callback([&](const std::string&, creek::Bytes p) {
        std::lock_guard<std::mutex> lk(mtx);
        msgs.emplace_back(p.begin(), p.end());
    });
    ASSERT_TRUE(cli.start());

    creek::RemotePeer rp;
    rp.id = "gw-27000";
    rp.address = creek::NetAddress("127.0.0.1", 27000);
    ASSERT_TRUE(cli.connect(rp));

    for (int i = 0; i < 30; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        if (ev.load() > 0) break;
    }
    ASSERT_GE(ev.load(), 1);

    std::string hello = "{\"type\":\"hello\",\"product_id\":\"p\","
        "\"product_key\":\"k\",\"product_secret\":\"s\","
        "\"device_name\":\"dev\"}";
    cli.send("gw-27000", creek::Bytes(hello.begin(), hello.end()));

    wait_msgs(msgs, mtx, 30);
    {
        std::lock_guard<std::mutex> lk(mtx);
        ASSERT_GE(msgs.size(), 1u);
        bool ok = false;
        for (auto& s : msgs) if (s.find("hello_ack") != std::string::npos) ok = true;
        ASSERT_TRUE(ok);
    }

    cli.stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    gw.stop();
}

TEST_CASE("e2e_ping_pong") {
    auto cfg = mk_cfg(27002);
    cfg.m_tight.id = "gw-27002";
    gateway::GatewayServer gw(cfg);
    ASSERT_TRUE(gw.start());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    creek::TightConfig cc;
    cc.bind = creek::NetAddress("127.0.0.1", 27003);
    cc.id = "pingdev";
    cc.token = "tok";
    cc.role = creek::LinkRole::Leaf;
    cc.mtu = 1400;
    cc.dead_timeout = std::chrono::seconds(30);

    std::vector<std::string> msgs;
    std::mutex mtx;
    std::atomic<int> ev{0};
    creek::TightTransport cli(cc);
    cli.set_peer_callback([&](const creek::PeerEvent&) { ++ev; });
    cli.set_message_callback([&](const std::string&, creek::Bytes p) {
        std::lock_guard<std::mutex> lk(mtx);
        msgs.emplace_back(p.begin(), p.end());
    });
    ASSERT_TRUE(cli.start());

    creek::RemotePeer rp;
    rp.id = "gw-27002";
    rp.address = creek::NetAddress("127.0.0.1", 27002);
    ASSERT_TRUE(cli.connect(rp));

    for (int i = 0; i < 30; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        if (ev.load() > 0) break;
    }
    ASSERT_GE(ev.load(), 1);

    std::string hello = "{\"type\":\"hello\",\"product_id\":\"p\","
        "\"product_key\":\"k\",\"product_secret\":\"s\","
        "\"device_name\":\"pingdev\"}";
    cli.send("gw-27002", creek::Bytes(hello.begin(), hello.end()));
    wait_msgs(msgs, mtx, 30);

    msgs.clear();
    std::string ping = "{\"type\":\"ping\",\"device_id\":\"pingdev\"}";
    cli.send("gw-27002", creek::Bytes(ping.begin(), ping.end()));

    wait_msgs(msgs, mtx, 15);
    {
        std::lock_guard<std::mutex> lk(mtx);
        bool ok = false;
        for (auto& s : msgs) if (s.find("pong") != std::string::npos) ok = true;
        ASSERT_TRUE(ok);
    }

    cli.stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    gw.stop();
}

TEST_CASE("e2e_downstream_fwd") {
    auto cfg = mk_cfg(27004);
    cfg.m_tight.id = "gw-27004";
    gateway::GatewayServer gw(cfg);
    std::atomic<int> ds{0};
    std::vector<gateway::GatewayMessage> dmsgs;
    std::mutex dm;
    gw.set_downstream_callback([&](const gateway::GatewayMessage& m) {
        ++ds;
        std::lock_guard<std::mutex> lk(dm);
        dmsgs.push_back(m);
    });
    ASSERT_TRUE(gw.start());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    creek::TightConfig cc;
    cc.bind = creek::NetAddress("127.0.0.1", 27005);
    cc.id = "dsdev";
    cc.token = "tok";
    cc.role = creek::LinkRole::Leaf;
    cc.mtu = 1400;
    cc.dead_timeout = std::chrono::seconds(30);

    std::vector<std::string> msgs;
    std::mutex mtx;
    std::atomic<int> ev{0};
    creek::TightTransport cli(cc);
    cli.set_peer_callback([&](const creek::PeerEvent&) { ++ev; });
    cli.set_message_callback([&](const std::string&, creek::Bytes p) {
        std::lock_guard<std::mutex> lk(mtx);
        msgs.emplace_back(p.begin(), p.end());
    });
    ASSERT_TRUE(cli.start());

    creek::RemotePeer rp;
    rp.id = "gw-27004";
    rp.address = creek::NetAddress("127.0.0.1", 27004);
    ASSERT_TRUE(cli.connect(rp));

    for (int i = 0; i < 30; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        if (ev.load() > 0) break;
    }
    ASSERT_GE(ev.load(), 1);

    std::string hello = "{\"type\":\"hello\",\"product_id\":\"p\","
        "\"product_key\":\"k\",\"product_secret\":\"s\","
        "\"device_name\":\"dsdev\"}";
    cli.send("gw-27004", creek::Bytes(hello.begin(), hello.end()));
    wait_msgs(msgs, mtx, 30);

    std::string cfg_upd = "{\"type\":\"config_update\",\"device_id\":\"dsdev\","
        "\"body\":{\"voice_type\":\"Warm_Girl\"}}";
    cli.send("gw-27004", creek::Bytes(cfg_upd.begin(), cfg_upd.end()));
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::string fc = "{\"type\":\"function_call_output\","
        "\"device_id\":\"dsdev\",\"call_id\":\"c1\",\"output\":\"ok\"}";
    cli.send("gw-27004", creek::Bytes(fc.begin(), fc.end()));
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    ASSERT_GE(ds.load(), 3);

    cli.stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    gw.stop();
}

TEST_CASE("e2e_auth_fail") {
    auto cfg = mk_cfg(27006);
    cfg.m_tight.id = "gw-27006";
    gateway::GatewayServer gw(cfg);
    ASSERT_TRUE(gw.start());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    creek::TightConfig cc;
    cc.bind = creek::NetAddress("127.0.0.1", 27007);
    cc.id = "baddev";
    cc.token = "tok";
    cc.role = creek::LinkRole::Leaf;
    cc.mtu = 1400;
    cc.dead_timeout = std::chrono::seconds(30);

    std::vector<std::string> msgs;
    std::mutex mtx;
    std::atomic<int> ev{0};
    creek::TightTransport cli(cc);
    cli.set_peer_callback([&](const creek::PeerEvent&) { ++ev; });
    cli.set_message_callback([&](const std::string&, creek::Bytes p) {
        std::lock_guard<std::mutex> lk(mtx);
        msgs.emplace_back(p.begin(), p.end());
    });
    ASSERT_TRUE(cli.start());

    creek::RemotePeer rp;
    rp.id = "gw-27006";
    rp.address = creek::NetAddress("127.0.0.1", 27006);
    ASSERT_TRUE(cli.connect(rp));

    for (int i = 0; i < 30; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        if (ev.load() > 0) break;
    }
    ASSERT_GE(ev.load(), 1);

    std::string bad = "{\"type\":\"hello\",\"product_id\":\"\","
        "\"product_key\":\"\",\"product_secret\":\"\","
        "\"device_name\":\"\"}";
    cli.send("gw-27006", creek::Bytes(bad.begin(), bad.end()));

    wait_msgs(msgs, mtx, 15);
    {
        std::lock_guard<std::mutex> lk(mtx);
        ASSERT_GE(msgs.size(), 1u);
        bool ok = false;
        for (auto& s : msgs) if (s.find("hello_err") != std::string::npos) ok = true;
        ASSERT_TRUE(ok);
    }

    cli.stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    gw.stop();
}

TEST_CASE("e2e_multi_device") {
    auto cfg = mk_cfg(27008);
    cfg.m_tight.id = "gw-27008";
    gateway::GatewayServer gw(cfg);
    ASSERT_TRUE(gw.start());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    constexpr int N = 3;
    struct Dev {
        std::unique_ptr<creek::TightTransport> t;
        std::vector<std::string> msgs;
        std::mutex mtx;
        std::atomic<int> ev{0};
    };
    Dev devs[N];

    for (int i = 0; i < N; ++i) {
        creek::TightConfig cc;
        cc.bind = creek::NetAddress("127.0.0.1",
                                     (std::uint16_t)(27009 + i * 2));
        cc.id = "dev" + std::to_string(i);
        cc.token = "tok";
        cc.role = creek::LinkRole::Leaf;
        cc.mtu = 1400;
        cc.dead_timeout = std::chrono::seconds(30);

        auto& d = devs[i];
        d.t = std::make_unique<creek::TightTransport>(cc);
        d.t->set_peer_callback([&](const creek::PeerEvent&) { ++d.ev; });
        d.t->set_message_callback([&](const std::string&, creek::Bytes p) {
            std::lock_guard<std::mutex> lk(d.mtx);
            d.msgs.emplace_back(p.begin(), p.end());
        });
        ASSERT_TRUE(d.t->start());

        creek::RemotePeer rp;
        rp.id = "gw-27008";
        rp.address = creek::NetAddress("127.0.0.1", 27008);
        ASSERT_TRUE(d.t->connect(rp));
    }

    for (int i = 0; i < N; ++i) {
        for (int j = 0; j < 30; ++j) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            if (devs[i].ev.load() > 0) break;
        }
        ASSERT_GE(devs[i].ev.load(), 1);
    }

    for (int i = 0; i < N; ++i) {
        std::string hello = "{\"type\":\"hello\",\"product_id\":\"p\","
            "\"product_key\":\"k\",\"product_secret\":\"s\","
            "\"device_name\":\"dev" + std::to_string(i) + "\"}";
        devs[i].t->send("gw-27008", creek::Bytes(hello.begin(), hello.end()));
    }

    for (int i = 0; i < N; ++i) {
        wait_msgs(devs[i].msgs, devs[i].mtx, 30);
        std::lock_guard<std::mutex> lk(devs[i].mtx);
        ASSERT_GE(devs[i].msgs.size(), 1u);
        bool ok = false;
        for (auto& s : devs[i].msgs)
            if (s.find("hello_ack") != std::string::npos) ok = true;
        ASSERT_TRUE(ok);
    }

    for (int i = 0; i < N; ++i) devs[i].t->stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    gw.stop();
}

TEST_CASE("e2e_full_flow") {
    auto cfg = mk_cfg(27015);
    cfg.m_tight.id = "gw-27015";
    gateway::GatewayServer gw(cfg);
    std::atomic<int> ds{0};
    gw.set_downstream_callback([&](const gateway::GatewayMessage&) { ++ds; });
    ASSERT_TRUE(gw.start());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    creek::TightConfig cc;
    cc.bind = creek::NetAddress("127.0.0.1", 27016);
    cc.id = "flodev";
    cc.token = "tok";
    cc.role = creek::LinkRole::Leaf;
    cc.mtu = 1400;
    cc.dead_timeout = std::chrono::seconds(30);

    std::vector<std::string> msgs;
    std::mutex mtx;
    std::atomic<int> ev{0};
    creek::TightTransport cli(cc);
    cli.set_peer_callback([&](const creek::PeerEvent&) { ++ev; });
    cli.set_message_callback([&](const std::string&, creek::Bytes p) {
        std::lock_guard<std::mutex> lk(mtx);
        msgs.emplace_back(p.begin(), p.end());
    });
    ASSERT_TRUE(cli.start());

    creek::RemotePeer rp;
    rp.id = "gw-27015";
    rp.address = creek::NetAddress("127.0.0.1", 27015);
    ASSERT_TRUE(cli.connect(rp));

    for (int i = 0; i < 30; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        if (ev.load() > 0) break;
    }
    ASSERT_GE(ev.load(), 1);

    std::string hello = "{\"type\":\"hello\",\"product_id\":\"p\","
        "\"product_key\":\"k\",\"product_secret\":\"s\","
        "\"device_name\":\"flodev\"}";
    cli.send("gw-27015", creek::Bytes(hello.begin(), hello.end()));
    wait_msgs(msgs, mtx, 30);

    msgs.clear();
    std::string ping = "{\"type\":\"ping\",\"device_id\":\"flodev\"}";
    cli.send("gw-27015", creek::Bytes(ping.begin(), ping.end()));
    wait_msgs(msgs, mtx, 10);
    {
        std::lock_guard<std::mutex> lk(mtx);
        bool ok = false;
        for (auto& s : msgs) if (s.find("pong") != std::string::npos) ok = true;
        ASSERT_TRUE(ok);
    }

    std::string cfg_upd = "{\"type\":\"config_update\",\"device_id\":\"flodev\","
        "\"body\":{\"voice_type\":\"Warm_Girl\"}}";
    cli.send("gw-27015", creek::Bytes(cfg_upd.begin(), cfg_upd.end()));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    std::string fc = "{\"type\":\"function_call_output\","
        "\"device_id\":\"flodev\",\"call_id\":\"c1\",\"output\":\"ok\"}";
    cli.send("gw-27015", creek::Bytes(fc.begin(), fc.end()));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    std::string bye = "{\"type\":\"bye\",\"device_id\":\"flodev\"}";
    cli.send("gw-27015", creek::Bytes(bye.begin(), bye.end()));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    ASSERT_GE(ds.load(), 4);

    cli.stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    gw.stop();
}
