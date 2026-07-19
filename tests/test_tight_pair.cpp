#include "tests/test_framework.hpp"
#include "creek/tight.hpp"
#include "creek/logger.hpp"

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

TEST_CASE("e2e_tight_transport_pair") {
    creek::TightConfig server_cfg;
    server_cfg.bind = creek::NetAddress("127.0.0.1", 20001);
    server_cfg.id = "server";
    server_cfg.token = "shared-secret";
    server_cfg.role = creek::LinkRole::Node;
    server_cfg.mtu = 1400;
    server_cfg.dead_timeout = std::chrono::seconds(30);

    std::atomic<int> server_msg_count{0};
    std::string server_last_msg;
    std::atomic<int> server_peer_events{0};
    std::atomic<int> server_cmd_count{0};

    creek::TightTransport server(server_cfg);
    server.set_message_callback(
        [&](const std::string& pid, creek::Bytes p) {
            ++server_msg_count;
            server_last_msg = std::string(p.begin(), p.end());
            server.send(pid, creek::Bytes(p.begin(), p.end()));
        });
    server.set_command_callback(
        [&](const std::string& pid, creek::Bytes p) {
            ++server_cmd_count;
            // Respond to the command over the command channel.
            server.send_command(pid, creek::Bytes(p.begin(), p.end()));
        });
    server.set_peer_callback(
        [&](const creek::PeerEvent& ev) {
            ++server_peer_events;
        });
    ASSERT_TRUE(server.start());

    creek::TightConfig client_cfg;
    client_cfg.bind = creek::NetAddress("127.0.0.1", 20002);
    client_cfg.id = "client";
    client_cfg.token = "shared-secret";
    client_cfg.role = creek::LinkRole::Leaf;
    client_cfg.mtu = 1400;
    client_cfg.dead_timeout = std::chrono::seconds(30);

    std::atomic<int> client_msg_count{0};
    std::string client_last_msg;
    std::atomic<int> client_cmd_count{0};
    std::string client_last_cmd;

    creek::TightTransport client(client_cfg);
    client.set_message_callback(
        [&](const std::string&, creek::Bytes p) {
            ++client_msg_count;
            client_last_msg = std::string(p.begin(), p.end());
        });
    client.set_command_callback(
        [&](const std::string&, creek::Bytes p) {
            ++client_cmd_count;
            client_last_cmd = std::string(p.begin(), p.end());
        });
    ASSERT_TRUE(client.start());

    creek::RemotePeer server_peer;
    server_peer.id = "server";
    server_peer.address = creek::NetAddress("127.0.0.1", 20001);
    ASSERT_TRUE(client.connect(server_peer));

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::string test_msg = "hello-from-client";
    client.send("server", creek::Bytes(test_msg.begin(), test_msg.end()));

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    ASSERT_GE(server_msg_count.load(), 1);
    ASSERT_GE(client_msg_count.load(), 1);
    ASSERT_EQ(server_last_msg, "hello-from-client");
    ASSERT_EQ(client_last_msg, "hello-from-client");

    // Command channel: the server echoes commands back over the command
    // channel.
    std::string cmd = "cmd-button-1";
    ASSERT_TRUE(client.send_command("server", creek::Bytes(cmd.begin(), cmd.end())));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    ASSERT_GE(server_cmd_count.load(), 1);
    ASSERT_GE(client_cmd_count.load(), 1);
    ASSERT_EQ(client_last_cmd, "cmd-button-1");

    // Oversized command (does not fit one datagram) is rejected.
    creek::Bytes big(2000, 0x41);
    ASSERT_FALSE(client.send_command("server", big));

    client.stop();
    server.stop();
}
