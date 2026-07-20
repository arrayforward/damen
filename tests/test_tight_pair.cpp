#include "tests/test_framework.hpp"
#include "tight/tight.hpp"
#include "tight/logger.hpp"

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

TEST_CASE("e2e_tight_transport_pair") {
    tight::TightConfig server_cfg;
    server_cfg.bind = tight::NetAddress("127.0.0.1", 20001);
    server_cfg.id = "server";
    server_cfg.token = "shared-secret";
    server_cfg.role = tight::LinkRole::Node;
    server_cfg.mtu = 1400;
    server_cfg.dead_timeout = std::chrono::seconds(30);

    std::atomic<int> server_msg_count{0};
    std::string server_last_msg;
    std::atomic<int> server_peer_events{0};
    std::atomic<int> server_cmd_count{0};

    tight::TightTransport server(server_cfg);
    server.set_message_callback(
        [&](const std::string& pid, tight::Bytes p) {
            ++server_msg_count;
            server_last_msg = std::string(p.begin(), p.end());
            server.send(pid, tight::Bytes(p.begin(), p.end()));
        });
    server.set_command_callback(
        [&](const std::string& pid, tight::Bytes p) {
            ++server_cmd_count;
            // Respond to the command over the command channel.
            server.send_command(pid, tight::Bytes(p.begin(), p.end()));
        });
    server.set_peer_callback(
        [&](const tight::PeerEvent& ev) {
            ++server_peer_events;
        });
    ASSERT_TRUE(server.start());

    tight::TightConfig client_cfg;
    client_cfg.bind = tight::NetAddress("127.0.0.1", 20002);
    client_cfg.id = "client";
    client_cfg.token = "shared-secret";
    client_cfg.role = tight::LinkRole::Leaf;
    client_cfg.mtu = 1400;
    client_cfg.dead_timeout = std::chrono::seconds(30);

    std::atomic<int> client_msg_count{0};
    std::string client_last_msg;
    std::atomic<int> client_cmd_count{0};
    std::string client_last_cmd;

    tight::TightTransport client(client_cfg);
    client.set_message_callback(
        [&](const std::string&, tight::Bytes p) {
            ++client_msg_count;
            client_last_msg = std::string(p.begin(), p.end());
        });
    client.set_command_callback(
        [&](const std::string&, tight::Bytes p) {
            ++client_cmd_count;
            client_last_cmd = std::string(p.begin(), p.end());
        });
    ASSERT_TRUE(client.start());

    tight::RemotePeer server_peer;
    server_peer.id = "server";
    server_peer.address = tight::NetAddress("127.0.0.1", 20001);
    ASSERT_TRUE(client.connect(server_peer));

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::string test_msg = "hello-from-client";
    client.send("server", tight::Bytes(test_msg.begin(), test_msg.end()));

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    ASSERT_GE(server_msg_count.load(), 1);
    ASSERT_GE(client_msg_count.load(), 1);
    ASSERT_EQ(server_last_msg, "hello-from-client");
    ASSERT_EQ(client_last_msg, "hello-from-client");

    // Command channel: the server echoes commands back over the command
    // channel.
    std::string cmd = "cmd-button-1";
    ASSERT_TRUE(client.send_command("server", tight::Bytes(cmd.begin(), cmd.end())));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    ASSERT_GE(server_cmd_count.load(), 1);
    ASSERT_GE(client_cmd_count.load(), 1);
    ASSERT_EQ(client_last_cmd, "cmd-button-1");

    // Oversized command (does not fit one datagram) is rejected.
    tight::Bytes big(2000, 0x41);
    ASSERT_FALSE(client.send_command("server", big));

    client.stop();
    server.stop();
}

TEST_CASE("e2e_tight_pair_lite_mode_and_dynamic_switch") {
    // 服务器普通模式（4 线程），客户端精简模式（2 线程）；
    // 验证精简模式回环 + 运行时 lite<->full 动态切换后收发正常
    tight::TightConfig server_cfg;
    server_cfg.bind = tight::NetAddress("127.0.0.1", 20021);
    server_cfg.id = "full-server";
    server_cfg.token = "shared-secret";
    server_cfg.role = tight::LinkRole::Node;

    tight::TightTransport server(server_cfg);
    server.set_message_callback(
        [&](const std::string& pid, tight::Bytes p) {
            server.send(pid, tight::Bytes(p.begin(), p.end()));
        });
    ASSERT_TRUE(server.start());
    ASSERT_FALSE(server.lite_mode());   // 服务器保持普通模式

    tight::TightConfig client_cfg;
    client_cfg.bind = tight::NetAddress("127.0.0.1", 20022);
    client_cfg.id = "lite-client";
    client_cfg.token = "shared-secret";
    client_cfg.role = tight::LinkRole::Leaf;
    client_cfg.lite_mode = true;        // 客户端精简模式

    tight::TightTransport client(client_cfg);
    std::atomic<int> echoes{0};
    std::string last;
    client.set_message_callback(
        [&](const std::string&, tight::Bytes p) {
            ++echoes;
            last = std::string(p.begin(), p.end());
        });
    ASSERT_TRUE(client.start());
    ASSERT_TRUE(client.lite_mode());
    ASSERT_TRUE(client.connect({"full-server", tight::NetAddress("127.0.0.1", 20021)}));
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto roundtrip = [&](const std::string& tag, int expect) {
        client.send("full-server", tight::Bytes(tag.begin(), tag.end()));
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
        ASSERT_EQ(echoes.load(), expect);
        ASSERT_EQ(last, tag);
    };

    // 精简模式下收发
    roundtrip("lite-roundtrip", 1);

    // 动态升级为完整模式
    client.set_lite_mode(false);
    ASSERT_FALSE(client.lite_mode());
    roundtrip("full-after-upgrade", 2);

    // 动态降级回精简模式
    client.set_lite_mode(true);
    ASSERT_TRUE(client.lite_mode());
    roundtrip("lite-after-downgrade", 3);

    client.stop();
    server.stop();
}

TEST_CASE("e2e_tight_pair_encryption_disabled") {
    // 关闭 ECDH+AEAD 后回退明文传输，确认兼容路径仍然可用
    tight::TightConfig server_cfg;
    server_cfg.bind = tight::NetAddress("127.0.0.1", 20011);
    server_cfg.id = "plain-server";
    server_cfg.token = "shared-secret";
    server_cfg.role = tight::LinkRole::Node;
    server_cfg.encryption_enabled = false;

    tight::TightTransport server(server_cfg);
    std::atomic<int> server_msgs{0};
    server.set_message_callback(
        [&](const std::string& pid, tight::Bytes p) {
            ++server_msgs;
            server.send(pid, tight::Bytes(p.begin(), p.end()));
        });
    ASSERT_TRUE(server.start());

    tight::TightConfig client_cfg;
    client_cfg.bind = tight::NetAddress("127.0.0.1", 20012);
    client_cfg.id = "plain-client";
    client_cfg.token = "shared-secret";
    client_cfg.role = tight::LinkRole::Leaf;
    client_cfg.encryption_enabled = false;

    tight::TightTransport client(client_cfg);
    std::atomic<int> client_msgs{0};
    std::string client_last;
    client.set_message_callback(
        [&](const std::string&, tight::Bytes p) {
            ++client_msgs;
            client_last = std::string(p.begin(), p.end());
        });
    ASSERT_TRUE(client.start());
    ASSERT_TRUE(client.connect({"plain-server", tight::NetAddress("127.0.0.1", 20011)}));

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    std::string msg = "plaintext-roundtrip";
    client.send("plain-server", tight::Bytes(msg.begin(), msg.end()));
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    ASSERT_GE(server_msgs.load(), 1);
    ASSERT_GE(client_msgs.load(), 1);
    ASSERT_EQ(client_last, "plaintext-roundtrip");

    client.stop();
    server.stop();
}
