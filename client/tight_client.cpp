// tight_client：tight 协议设备端直连测试工具
//
// 用法：
//   tight_client [--host 127.0.0.1] [--port 9443] [--token gateway-shared-secret]
//                [--id device-001] [--name device-001] [--mtu 1200]
//                [--role leaf|node] [--no-encryption] [--no-speed-test]
//
// 连接后进入交互模式，支持命令：
//   hello            发送入网认证（product 字段为演示值）
//   ping             发送 ping（等待 pong）
//   bye              发送 bye 下线
//   send <文本>      发送原始数据消息
//   audio [帧数]     发送音频帧（默认 10 帧，160 字节 g711 模拟数据）
//   config <json>    发送 config_update
//   cmd <文本>       通过命令通道发送控制指令（保序、插队、单报文）
//   peers            显示当前链路状态
//   help             显示帮助
//   quit / exit      退出
//
// 收到的数据消息 / 命令 / 链路事件实时打印到控制台。

#include "tight/tight.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {

std::mutex g_console_mutex;

// 线程安全控制台输出（回调来自 transport 内部线程）
void print(const std::string& line) {
    std::lock_guard<std::mutex> lock(g_console_mutex);
    std::cout << line << std::endl;
}

std::string bytes_to_string(const tight::Bytes& b) {
    return std::string(b.begin(), b.end());
}

void print_usage(const char* argv0) {
    std::cout << "用法: " << argv0 << " [选项]\n"
              << "  --host <ip>        网关地址（默认 127.0.0.1）\n"
              << "  --port <端口>      网关端口（默认 9443）\n"
              << "  --token <令牌>     接入令牌（默认 gateway-shared-secret）\n"
              << "  --id <标识>        本端 tight id（默认 device-001）\n"
              << "  --name <设备名>    hello 中的 device_name（默认同 id）\n"
              << "  --mtu <字节>       报文 MTU（默认 1200）\n"
              << "  --role <leaf|node> 链路角色（默认 leaf；node 掉线自动重连）\n"
              << "  --no-encryption    关闭 ECDH+AES-256-GCM 加密\n"
              << "  --no-speed-test    关闭建连测速\n"
              << "  --help             显示本帮助\n";
}

void print_help() {
    std::cout << "命令: hello | ping | bye | send <文本> | audio [帧数] |\n"
              << "      config <json> | cmd <文本> | peers | help | quit\n";
}

} // namespace

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    std::uint16_t port = 9443;
    std::string token = "gateway-shared-secret";
    std::string id = "device-001";
    std::string name;
    std::size_t mtu = 1200;
    tight::LinkRole role = tight::LinkRole::Leaf;
    bool encryption = true;
    bool speed_test = true;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&](const char* err) -> std::string {
            if (i + 1 >= argc) {
                std::cerr << "选项 " << arg << " 缺少参数（" << err << "）\n";
                std::exit(2);
            }
            return argv[++i];
        };
        if (arg == "--host") host = next("网关地址");
        else if (arg == "--port") port = static_cast<std::uint16_t>(std::stoi(next("端口")));
        else if (arg == "--token") token = next("令牌");
        else if (arg == "--id") id = next("标识");
        else if (arg == "--name") name = next("设备名");
        else if (arg == "--mtu") mtu = static_cast<std::size_t>(std::stoul(next("MTU")));
        else if (arg == "--role") {
            std::string r = next("leaf|node");
            role = (r == "node") ? tight::LinkRole::Node : tight::LinkRole::Leaf;
        }
        else if (arg == "--no-encryption") encryption = false;
        else if (arg == "--no-speed-test") speed_test = false;
        else if (arg == "--help") { print_usage(argv[0]); return 0; }
        else {
            std::cerr << "未知选项: " << arg << "\n";
            print_usage(argv[0]);
            return 2;
        }
    }
    if (name.empty()) name = id;

    tight::TightConfig cfg;
    cfg.bind = tight::NetAddress("0.0.0.0", 0);
    cfg.id = id;
    cfg.token = token;
    cfg.role = role;
    cfg.mtu = mtu;
    cfg.encryption_enabled = encryption;
    cfg.speed_test_enabled = speed_test;

    std::atomic<bool> online{false};

    tight::TightTransport transport(cfg);
    std::string gateway_id = "gateway-" + std::to_string(port);

    transport.set_message_callback(
        [&](const std::string& peer, tight::Bytes payload) {
            print("[数据] 来自 " + peer + " : " + bytes_to_string(payload));
        });
    transport.set_command_callback(
        [&](const std::string& peer, tight::Bytes payload) {
            print("[命令] 来自 " + peer + " : " + bytes_to_string(payload));
        });
    transport.set_peer_callback(
        [&](const tight::PeerEvent& ev) {
            const char* state = "Unknown";
            switch (ev.state) {
            case tight::LinkState::Closed:      state = "Closed";      break;
            case tight::LinkState::Handshake:   state = "Handshake";   break;
            case tight::LinkState::Established: state = "Established"; break;
            case tight::LinkState::Online:      state = "Online";      break;
            }
            if (ev.state == tight::LinkState::Online) online.store(true);
            if (ev.state == tight::LinkState::Closed) online.store(false);
            print("[事件] 对端 " + ev.id + " 状态 -> " + state);
        });

    if (!transport.start()) {
        std::cerr << "transport 启动失败\n";
        return 1;
    }
    print("[信息] 本地端口 " + std::to_string(transport.local_port()) +
          "，正在连接 " + host + ":" + std::to_string(port) + " ...");

    tight::RemotePeer gateway{gateway_id, tight::NetAddress(host, port)};
    if (!transport.connect(gateway)) {
        std::cerr << "connect 失败\n";
        return 1;
    }

    print_help();
    std::string line;
    while (true) {
        {
            std::lock_guard<std::mutex> lock(g_console_mutex);
            std::cout << "> " << std::flush;
        }
        if (!std::getline(std::cin, line)) break;   // EOF（管道输入结束）
        if (line.empty()) continue;

        auto sp = line.find(' ');
        std::string verb = sp == std::string::npos ? line : line.substr(0, sp);
        std::string rest = sp == std::string::npos ? "" : line.substr(sp + 1);

        if (verb == "quit" || verb == "exit") {
            break;
        } else if (verb == "help") {
            print_help();
        } else if (verb == "hello") {
            std::string json = "{\"type\":\"hello\",\"product_id\":\"demo-pid\","
                               "\"product_key\":\"demo-key\",\"product_secret\":\"demo-secret\","
                               "\"device_name\":\"" + name + "\"}";
            bool ok = transport.send(gateway_id, tight::Bytes(json.begin(), json.end()));
            print(ok ? "[发送] hello" : "[错误] hello 发送失败（未 Online 或队列满）");
        } else if (verb == "ping") {
            std::string json = "{\"type\":\"ping\",\"device_id\":\"" + name + "\"}";
            bool ok = transport.send(gateway_id, tight::Bytes(json.begin(), json.end()));
            print(ok ? "[发送] ping" : "[错误] ping 发送失败");
        } else if (verb == "bye") {
            std::string json = "{\"type\":\"bye\",\"device_id\":\"" + name + "\"}";
            bool ok = transport.send(gateway_id, tight::Bytes(json.begin(), json.end()));
            print(ok ? "[发送] bye" : "[错误] bye 发送失败");
        } else if (verb == "send") {
            if (rest.empty()) { print("[提示] 用法: send <文本>"); continue; }
            bool ok = transport.send(gateway_id, tight::Bytes(rest.begin(), rest.end()));
            print(ok ? "[发送] 数据 " + std::to_string(rest.size()) + " 字节"
                     : "[错误] 发送失败");
        } else if (verb == "audio") {
            int frames = rest.empty() ? 10 : std::stoi(rest);
            tight::Bytes frame(160, 0xD5);   // g711a 静音帧模拟
            int sent = 0;
            for (int i = 0; i < frames; ++i) {
                if (transport.send(gateway_id, tight::Bytes(frame))) ++sent;
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
            print("[发送] 音频帧 " + std::to_string(sent) + "/" + std::to_string(frames));
        } else if (verb == "config") {
            std::string body = rest.empty() ? "{}" : rest;
            std::string json = "{\"type\":\"config_update\",\"device_id\":\"" + name +
                               "\",\"body\":" + body + "}";
            bool ok = transport.send(gateway_id, tight::Bytes(json.begin(), json.end()));
            print(ok ? "[发送] config_update" : "[错误] 发送失败");
        } else if (verb == "cmd") {
            if (rest.empty()) { print("[提示] 用法: cmd <文本>"); continue; }
            bool ok = transport.send_command(gateway_id,
                                             tight::Bytes(rest.begin(), rest.end()));
            print(ok ? "[发送] 命令 " + std::to_string(rest.size()) + " 字节"
                     : "[错误] 命令发送失败（超单报文上限或未 Online）");
        } else if (verb == "peers") {
            for (const auto& ev : transport.peers()) {
                print("[链路] " + ev.id + " state=" +
                      std::to_string(static_cast<int>(ev.state)) +
                      (online.load() ? "（本端 Online）" : ""));
            }
        } else {
            print("[提示] 未知命令，输入 help 查看帮助");
        }
    }

    print("[信息] 正在退出 ...");
    transport.stop();
    return 0;
}
