// 双进程内存测试：server / client 分离，各自只测自身进程工作集。
// 用法：先起 server，再起 client（lite 模式，8KB 消息上限）。
#include "tight/tight.hpp"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <windows.h>
#include <psapi.h>

using namespace tight;

static std::size_t working_set_kb() {
    PROCESS_MEMORY_COUNTERS pmc{};
    GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc));
    return pmc.WorkingSetSize / 1024;
}

static int run_server() {
    TightConfig sc;
    sc.bind = NetAddress("127.0.0.1", 21070);
    sc.id = "srv";
    sc.token = "token";
    sc.role = LinkRole::Node;
    sc.max_message_bytes = 8 * 1024;
    sc.speed_test_enabled = false;
    sc.encryption_enabled = false;
    TightTransport server(sc);
    server.set_message_callback([](const std::string&, Bytes) {});
    if (!server.start()) return 1;
    std::printf("[server] ready\n");
    std::this_thread::sleep_for(std::chrono::minutes(3));
    server.stop();
    return 0;
}

static int run_client() {
    TightConfig cc;
    cc.bind = NetAddress("127.0.0.1", 21071);
    cc.id = "cli";
    cc.token = "token";
    cc.max_message_bytes = 8 * 1024;
    cc.speed_test_enabled = false;
    cc.encryption_enabled = false;
    cc.lite_mode = true;
    TightTransport client(cc);
    if (!client.start()) return 1;
    if (!client.connect({"srv", NetAddress("127.0.0.1", 21070)})) return 1;
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::printf("[client] connected WS: %zu KB\n", working_set_kb());

    Bytes msg(8192, 'x');
    for (std::size_t i = 0; i < msg.size(); i += 64) msg[i] = static_cast<char>(i / 64);

    std::size_t base = working_set_kb();
    std::printf("[client] baseline WS: %zu KB\n", base);

    // 限速 50KB/s：每 8KB 消息间隔 8192*1000/51200 = 160ms
    constexpr int kCount = 200;
    const auto interval = std::chrono::microseconds(8192LL * 1000000 / 51200);
    std::size_t sent = 0;
    std::size_t peak = base;
    auto next_send = std::chrono::steady_clock::now();
    for (int i = 0; i < kCount; ++i) {
        next_send += interval;
        std::this_thread::sleep_until(next_send);
        while (!client.send("srv", msg)) std::this_thread::sleep_for(std::chrono::milliseconds(5));
        ++sent;
        peak = std::max(peak, working_set_kb());
        if (sent % 50 == 0) {
            std::printf("[client] sent %zu/%d WS: %zu KB\n", sent, kCount, working_set_kb());
        }
    }
    std::this_thread::sleep_for(std::chrono::seconds(2));
    peak = std::max(peak, working_set_kb());
    std::printf("[client] after %dx8KB @50KB/s peak WS: %zu KB (delta %zd KB)\n",
                sent, peak,
                static_cast<std::ptrdiff_t>(peak) - static_cast<std::ptrdiff_t>(base));

    client.stop();
    std::printf("[client] after stop WS: %zu KB\n", working_set_kb());
    return 0;
}

int main(int argc, char** argv) {
    if (argc < 2) { std::printf("usage: test_mem8k_split server|client\n"); return 2; }
    if (std::strcmp(argv[1], "server") == 0) return run_server();
    if (std::strcmp(argv[1], "client") == 0) return run_client();
    return 2;
}
