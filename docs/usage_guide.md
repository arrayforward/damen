# 使用说明

本文档覆盖 damen 的完整使用流程：构建、运行、设备接入、命令通道、网关扩展与配置调优。

---

## 1. 构建

### 1.1 环境要求

| 依赖 | 说明 |
|---|---|
| C++17 编译器 | Windows 推荐 WinLibs MinGW（`winget install BrechtSanders.WinLibs.POSIX.UCRT`）；Linux 用系统 g++ |
| CMake ≥ 3.16 | — |
| 系统库 | Windows：`ws2_32`（自动链接）；Linux：`pthread`（自动链接） |

无第三方库依赖（加密原语为内建纯 C++ 实现）。

### 1.2 Windows 一键构建

```bat
build.bat              rem Release 构建到 build/
build.bat test         rem 构建 + 运行全部 12 个测试套件
build.bat clean        rem 清理 build/ 后构建
build.bat debug        rem Debug 构建（可组合：build.bat clean debug test）
```

脚本自动探测 `cmake` 与 `g++`；`g++` 不在 PATH 时回退到 winget 默认 WinLibs 路径。

### 1.3 CMake 手动构建

```bash
# Windows (MinGW)
cmake -B build -S . -G "MinGW Makefiles" -DCMAKE_CXX_COMPILER=g++
cmake --build build -j 4

# Linux
cmake -B build -S .
cmake --build build -j$(nproc)

# 测试
ctest --test-dir build --output-on-failure
```

构建产物：`build/cloud_gateway.exe`（网关主程序）、`build/libcreek.a`（tight 静态库）、各测试可执行文件。

---

## 2. 运行网关

```bash
./build/cloud_gateway.exe
```

默认行为（`gateway/gateway_main.cpp`，可修改后重新编译）：

| 项 | 默认值 |
|---|---|
| tight UDP 监听 | `0.0.0.0:9443` |
| 接入 token | `gateway-shared-secret` |
| mtu | 1400 |
| 心跳 / 掉线超时 | 5s / 30s |
| 指标 HTTP | `:9090/metrics`（Prometheus 文本） |
| 日志级别 | Debug |

验证指标：

```bash
curl http://127.0.0.1:9090/metrics
# gateway_active_connections 0
# gateway_messages_in_total 0 ...
```

`Ctrl+C` 触发优雅退出（广播 Bye、关闭 reactor）。

---

## 3. 设备接入

### 3.1 最小接入流程

```cpp
#include "creek/tight.hpp"

creek::TightConfig cfg;
cfg.bind  = creek::NetAddress("0.0.0.0", 0);
cfg.id    = "device-001";
cfg.token = "gateway-shared-secret";
cfg.role  = creek::LinkRole::Leaf;

creek::TightTransport t(cfg);
t.set_message_callback([](const std::string& peer, creek::Bytes payload) {
    std::string json(payload.begin(), payload.end());
    // hello_ack / status / event / text / function_call / pong ...
});
t.set_peer_callback([](const creek::PeerEvent& ev) {
    if (ev.state == creek::LinkState::Online)      { /* 已上线 */ }
    if (ev.state == creek::LinkState::Closed)      { /* 已离线 */ }
});
t.set_command_callback([](const std::string& peer, creek::Bytes payload) {
    // 保序命令（云端控制/按键）
});

if (!t.start()) return 1;
t.connect({"gateway-9443", creek::NetAddress("网关IP", 9443)});

// 入网（JSON 协议见 3.3）
std::string hello = R"({"type":"hello","product_id":"p1","product_key":"k1",
                        "product_secret":"s1","device_name":"device-001"})";
t.send("gateway-9443", creek::Bytes(hello.begin(), hello.end()));
```

握手成功后自动完成：ECDH 密钥协商（数据加密开启时）→ 双向 100KB 测速 → 带宽基线校准。业务无需干预。

### 3.2 连接可靠性

- 断线重连：`role = Node` 的一端在 `dead_timeout` 后自动重连（建议设备端设 Node）
- 应用层保活：网关每 2×`heartbeat_seconds` 未收到设备活动则关闭会话；设备定期发送任何消息即可
- 优雅下线：发送 `{"type":"bye","device_id":"..."}` 或析构 transport（自动广播 Bye）

### 3.3 上行消息（设备 → 网关，JSON）

| type | 字段 | 说明 |
|---|---|---|
| `hello` | product_id, product_key, product_secret, device_name | 入网认证，成功回 `hello_ack`，失败回 `hello_err` |
| `ping` | device_id | 回 `pong` |
| `config_update` | device_id, body | 透传下行 |
| `function_call_output` | device_id, call_id, output | 透传下行 |
| `audio_frame` | device_id | 音频帧（带宽限流 + 计费）后下行 |
| `audio_boundary` | device_id, event | 透传下行 |
| `bye` | device_id | 关闭会话 |

### 3.4 下行消息（网关 → 设备，JSON）

`hello_ack`（device_id, session_id）、`hello_err`（body.code）、`status`、`event`、`text`、`function_call`、`config_update_ack`、`config_update_err`、`audio_chunk`（原始负载）、`pong`。

### 3.5 命令通道（控制/按键）

```cpp
// 发送（单报文 ≤ mtu-48；默认 mtu 1200 时 ≤ 1152 字节，超限返回 false）
t.send_command("gateway-9443", {'v','o','l','+',});

// 接收（保序投递；乱序最多等待 3×RTT，缺口超时跳过，迟到包丢弃）
t.set_command_callback([](const std::string& peer, creek::Bytes payload) { ... });
```

命令与数据共用加密通道（ECDH 协商后自动加密），但**不占数据序列号、不走 FEC/重组**，发送时插队优先。

### 3.6 常用配置（`TightConfig`）

```cpp
cfg.mtu = 1200;                       // 报文大小（含 48 字节头）
cfg.late_rtt_multiplier = 4.0;        // 慢包阈值（传输时间 > 4×RTT）
cfg.speed_test_enabled = true;        // 建连测速
cfg.speed_test_bytes = 100 * 1024;    // 测速列车大小
cfg.encryption_enabled = true;        // ECDH + AES-256-GCM
cfg.initial_bandwidth_bytes = 100 * 1024 * 1024;  // 初始限速（测速后更新）
cfg.heartbeat = std::chrono::seconds(5);
cfg.dead_timeout = std::chrono::seconds(30);
```

---

## 4. 网关扩展开发

### 4.1 注册自定义监听器

```cpp
gateway::GatewayServer server(config);

server.add_message_listener(gateway::GatewayMessageType::kDevicePing,
    [](const gateway::GatewayMessage& msg) {
        // 与内置 listener 同等地位；多 listener 按注册顺序调用
    });

server.start();
```

- 分发为快照迭代：listener 内可再注册，不会死锁
- 单个 listener 抛异常不影响其他 listener 与当前批次
- 需要发包/下行时，请通过 listener 上下文排队 IO（参照内置 listener 的 `m_ctx->m_schedule_io` 模式），批末统一执行

### 4.2 编写独立 listener 类（推荐）

参照 `gateway/hello_listener.hpp`：持有一个共享 `GatewayContext`，暴露 `listeners()` 注册表，由 server 统一注册。listener 不反向依赖 `GatewayServer`。

### 4.3 注入鉴权逻辑

`GatewayServer::start()` 中创建的 `AuthService` 默认校验字段非空。接入真实账号体系时，替换 `gateway_main.cpp`（或自定义入口）中 `do_authenticate` 的实现，返回带 JWT/过期时间的 `AuthResult`。

### 4.4 指标与限流

```cpp
curl http://127.0.0.1:9090/metrics     // Prometheus 文本
server.ratelimiter()->configure("bandwidth", {64 * 1024.0, 64 * 1024.0,
                                              std::chrono::seconds(1)});
```

---

## 5. 测试与仿真

### 5.1 测试套件

```bash
ctest --test-dir build --output-on-failure      # 全部 12 个套件
ctest --test-dir build -R test_e2e_sim          # 单个套件
.\build\test_e2e_sim.exe                        # 直接运行查看详细输出
```

### 5.2 用 SimDevice 编写自己的验证

```cpp
#include "tests/sim_device.hpp"

gateway::SimDevice dev("dev-1", 9443);       // 连接本地 9443 网关
dev.start();
dev.wait_for_connected();
dev.send_hello("pid", "key", "secret");
dev.wait_for_json_containing("hello_ack");
dev.send_ping();
dev.wait_for_json_containing("pong");
```

---

## 6. 故障排查

| 现象 | 排查 |
|---|---|
| 设备连不上 | token 是否一致；网关端口/防火墙；`id` 重复导致 peer 被顶替 |
| 收到 hello_err | `body.code`：`AUTH_FAILED`（字段缺失）/ `RATE_LIMITED`（连接限流 10s 冷却） |
| 丢包率高 | 提高 `initial_bandwidth_bytes` 或确认测速结果；查看网关日志 `Metrics|` 行 |
| 命令延迟/丢失 | 命令为单报文 UDP：乱序窗口 3×RTT 后跳过；检查是否超过 mtu−48 被 `send_command` 拒绝（返回 false） |
| 加密未启用 | 两端 `encryption_enabled` 需一致开启；低阶点公钥会自动回退明文并打 WARN 日志 |
| 调试日志 | `creek::Logger::instance().set_level(creek::LogLevel::Debug)` |
