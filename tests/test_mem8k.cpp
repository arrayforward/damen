#include "tight/tight.hpp"

#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#include <psapi.h>
static std::size_t working_set_kb() {
    PROCESS_MEMORY_COUNTERS pmc{};
    GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc));
    return pmc.WorkingSetSize / 1024;
}
#else
#include <fstream>
static std::size_t working_set_kb() {
    std::ifstream in("/proc/self/status");
    std::string key;
    std::size_t value;
    std::string unit;
    while (in >> key) {
        if (key == "VmRSS:") { in >> value >> unit; return value; }
        in.ignore(4096, '\n');
    }
    return 0;
}
#endif

using namespace tight;

int main() {
    TightConfig sc;
    sc.bind = NetAddress("127.0.0.1", 21060);
    sc.id = "srv";
    sc.token = "token";
    sc.role = LinkRole::Node;
    sc.max_message_bytes = 8 * 1024;
    sc.speed_test_enabled = false;
    sc.encryption_enabled = false;
    TightTransport server(sc);

    TightConfig cc;
    cc.bind = NetAddress("127.0.0.1", 21061);
    cc.id = "cli";
    cc.token = "token";
    cc.max_message_bytes = 8 * 1024;
    cc.speed_test_enabled = false;
    cc.encryption_enabled = false;
    cc.lite_mode = true;
    TightTransport client(cc);

    std::size_t received = 0;
    server.set_message_callback([&](const std::string&, Bytes) { ++received; });
    if (!server.start()) { std::printf("[mem8k] server start FAIL\n"); return 1; }
    if (!client.start()) { std::printf("[mem8k] client start FAIL\n"); return 1; }
    if (!client.connect({"srv", NetAddress("127.0.0.1", 21060)})) {
        std::printf("[mem8k] connect FAIL\n");
        return 1;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    Bytes msg(8192, 'x');
    for (std::size_t i = 0; i < msg.size(); i += 64) msg[i] = static_cast<char>(i / 64);

    std::size_t base = working_set_kb();
    std::printf("[mem8k] baseline WS: %zu KB\n", base);

    std::size_t sent = 0;
    for (int i = 0; i < 2000; ++i) {
        // 队列满时 send 返回 false，退避重试（内存测试关注驻留而非吞吐）
        while (!client.send("srv", msg)) std::this_thread::sleep_for(std::chrono::milliseconds(5));
        ++sent;
        // 自适应限速：数据面 best-effort+FEC 无重传，发送速率不得超过
        // 接收端排干速率，否则 socket 缓冲溢出即丢包（设计行为）。
        if (sent % 25 == 0) {
            auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
            while (received + 4 < sent && std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
        }
    }
    std::this_thread::sleep_for(std::chrono::seconds(2));
    std::size_t peak = working_set_kb();
    std::printf("[mem8k] after 2000x8KB WS: %zu KB (delta %zd KB)\n",
                peak, static_cast<std::ptrdiff_t>(peak) - static_cast<std::ptrdiff_t>(base));

    std::this_thread::sleep_for(std::chrono::seconds(1));
    client.stop();
    server.stop();

    std::size_t settle = working_set_kb();
    std::printf("[mem8k] after stop WS: %zu KB  received=%zu/%zu\n", settle, received, sent);

    bool ok = received >= sent && settle < peak + 4096;
    std::printf("[mem8k] %s\n", ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
