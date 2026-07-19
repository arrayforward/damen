#include "tests/test_framework.hpp"

#include <cstdio>
#include <chrono>
#include <string>
#include <thread>

#include "creek/tight.hpp"
#include "creek/types.hpp"

TEST_CASE("minimal_gateway_like_server") {
    creek::TightConfig srv_cfg;
    srv_cfg.bind = creek::NetAddress("127.0.0.1", 25000);
    srv_cfg.id = "srv";
    srv_cfg.token = "tok";
    srv_cfg.role = creek::LinkRole::Node;
    srv_cfg.mtu = 1400;

    creek::TightTransport srv(srv_cfg);
    std::atomic<int> srv_events{0};
    std::atomic<int> srv_msgs{0};
    srv.set_peer_callback([&](const creek::PeerEvent& ev) {
        std::printf("[SRV] peer event: state=%d id=%s\n",
                    (int)ev.state, ev.id.c_str());
        ++srv_events;
    });
    srv.set_message_callback([&](const std::string& pid, creek::Bytes p) {
        std::string s(p.begin(), p.end());
        std::printf("[SRV] msg from %s: %s\n", pid.c_str(), s.c_str());
        ++srv_msgs;
        srv.send(pid, creek::Bytes(p.begin(), p.end()));
    });
    ASSERT_TRUE(srv.start());
    std::printf("[SRV] started on port %u\n", srv.local_port());

    creek::TightConfig cli_cfg;
    cli_cfg.bind = creek::NetAddress("127.0.0.1", 25001);
    cli_cfg.id = "cli";
    cli_cfg.token = "tok";
    cli_cfg.role = creek::LinkRole::Leaf;
    cli_cfg.mtu = 1400;

    creek::TightTransport cli(cli_cfg);
    std::atomic<int> cli_events{0};
    std::atomic<int> cli_msgs{0};
    cli.set_peer_callback([&](const creek::PeerEvent& ev) {
        std::printf("[CLI] peer event: state=%d id=%s\n",
                    (int)ev.state, ev.id.c_str());
        ++cli_events;
    });
    cli.set_message_callback([&](const std::string&, creek::Bytes p) {
        std::string s(p.begin(), p.end());
        std::printf("[CLI] msg: %s\n", s.c_str());
        ++cli_msgs;
    });
    ASSERT_TRUE(cli.start());
    std::printf("[CLI] started\n");

    creek::RemotePeer rp;
    rp.id = "srv";
    rp.address = creek::NetAddress("127.0.0.1", srv.local_port());
    ASSERT_TRUE(cli.connect(rp));
    std::printf("[CLI] connected\n");

    std::this_thread::sleep_for(std::chrono::milliseconds(400));

    ASSERT_GE(srv_events.load(), 1);
    ASSERT_GE(cli_events.load(), 1);

    std::string msg = "hello-world";
    cli.send("srv", creek::Bytes(msg.begin(), msg.end()));

    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    ASSERT_GE(srv_msgs.load(), 1);
    ASSERT_GE(cli_msgs.load(), 1);

    std::printf("[TEST] srv_events=%d srv_msgs=%d cli_events=%d cli_msgs=%d\n",
                srv_events.load(), srv_msgs.load(),
                cli_events.load(), cli_msgs.load());

    cli.stop();
    srv.stop();
}
