# ConvAI Cloud Gateway（damen）

面向智能设备的云端网关服务。设备与云之间通过自研 **tight** 可靠 UDP 传输协议通信（ECDH 密钥交换 + AES-256-GCM 数据加密 + Reed-Solomon 前向纠错 + BBR 拥塞控制），网关基于反应式架构（Reactor + Channel + Blackboard）处理设备消息并向媒体/AI 下游服务转发。

- 语言标准：C++17（仅标准库 + 系统 socket API，**零第三方依赖**，含内建加密原语）
- 平台：Windows（Winsock2，`ws2_32`）/ Linux（POSIX socket，`pthread`）
- 构建：CMake（Windows 提供一键 `build.bat`）
- 测试：12 个 ctest 套件（单元 + 加密向量 + sim_device 端到端），全部通过

---

## 1. 核心特性

### 1.1 tight 传输协议

| 特性 | 实现位置 | 说明 |
|---|---|---|
| 可靠传输 | `tight/src/transport.cpp` `tight/src/report.cpp` | ACK 确认 + Report NACK 丢包列表重传（≤10 次），心跳保活，`dead_timeout` 掉线检测与自动重连 |
| **ECDH + AEAD** | `tight/src/crypto.{hpp,cpp}` | 握手交换 X25519 公钥（RFC 7748），HKDF-SHA256 派生会话密钥，Data/Parity/Command 负载 AES-256-GCM 加密，报文头前 44 字节作为 AAD 防篡改；`encryption_enabled` 可开关 |
| **Reed-Solomon FEC** | `tight/src/fec.cpp` `tight/src/fragmenter.cpp` `tight/src/reassembler.cpp` | GF(2⁸) Vandermonde 擦除码，p 个校验分片恢复任意 p 个丢失分片；冗余率 = 迟到率的二元信息熵 H(p) × 1.2 安全系数，clamp [1,3] |
| 慢包统计 | `tight/src/reassembler.cpp` | 用时钟偏移计算单向传输时间，超过 `late_rtt_multiplier`（默认 4）× RTT 判为慢包，按 report 周期上报比例 |
| 时钟对表 | `tight/src/transport.cpp` `tight/src/peer.hpp` | 握手对表（偏移 = 对端 tick − 本地到达 − RTT/2），每次心跳平滑重对表（7/8），只存偏移不改本地时间 |
| BBR 带宽估算 | `tight/src/bandwidth.cpp` | BtlBw 窗口最大值（10 样本）+ RTprop 最小 RTT；增益以 RTT 趋势为主信号、迟到率为辅助信号 |
| 建连测速 | `tight/src/transport.cpp` | Online 后双向互发 100KB Probe 空白列车（直发不限速），接收方按到达时间估算带宽并经 report 回传；`speed_test_enabled` 可开关 |
| 命令通道 | `tight/src/command.cpp` | 单报文控制/按键指令（≤ mtu−48），独立序列空间保序投递，乱序最多等待 3×RTT 后跳过缺口，插队直发出站队列 |
| 限速发送 | `tight/src/transport.cpp` | 令牌桶按估算带宽 pacing；reactor / receiver / encode / sender 四线程分离 |

### 1.2 网关服务

- **反应式架构**：CopyChannel 收件箱 → Reactor 主循环 10ms 批处理 → CPU 线程处理 → 批次末尾统一执行 IO；DataBoard 黑板持有设备/下游/指标状态
- **监听器观察者模式**：`add_message_listener(type, fn)` 按类型注册，支持多 listener、运行时再注册；内置 4 个监听器独立成文件：
  - `gateway/hello_listener.hpp` — 入网认证（hello 限流 + 鉴权 + hello_ack/err + 下行通知）
  - `gateway/audio_listener.hpp` — 音频帧（带宽限流 + 字节计费）与边界事件，下行转发
  - `gateway/device_listener.hpp` — bye 会话关闭、ping→pong、内部限流通知
  - `gateway/forward_listener.hpp` — config_update / function_call_output 透传下行
- **配套组件**：`SessionManager` 会话、`AuthService` 鉴权（可注入 validator）、`RateLimiter` 四维度令牌桶、`Metrics` Prometheus 文本 + `MetricsHttpServer`（默认 `:9090/metrics`）

---

## 2. 目录结构

```
damen/
├── tight/                       # ★ tight 协议独立库（namespace tight，可单独构建/复用）
│   ├── CMakeLists.txt           #   库独立构建文件（add_subdirectory / install 两用）
│   ├── include/tight/           #   公共接口头（对外 API）
│   │   ├── tight.hpp            #     TightTransport 主接口（聚合子头）
│   │   ├── types.hpp            #     基础类型 + TightConfig 全部配置项
│   │   ├── packet_codec.hpp     #     48 字节线格式编解码 + CRC32
│   │   ├── fec.hpp              #     ReedSolomon：GF(2⁸) 擦除码
│   │   ├── bandwidth.hpp        #     BandwidthEstimator：BBR 带宽估算器
│   │   ├── blocking_queue.hpp   #     BlockingQueue<T> 有界阻塞队列
│   │   └── logger.hpp           #     Logger 单例 + TIGHT_LOG_* 宏
│   └── src/                     #   私有实现（不对外暴露）
│       ├── transport.cpp        #     核心：线程模型、控制面、收发调度、ECDH/AEAD 挂钩
│       ├── crypto.{hpp,cpp}     #     X25519 / SHA-256 / HKDF / AES-256-GCM（纯 C++）
│       ├── reassembler.*        #     入向：序列跟踪 + 慢包统计 + 重组 + RS 恢复
│       ├── fragmenter.*         #     出向：分片 + 熵驱动 RS 冗余
│       ├── report.*             #     报告构建/处理（慢包率/丢包重传/带宽回传）
│       ├── command.*            #     命令通道：保序 + 3×RTT 乱序窗口
│       ├── packet_codec.cpp / fec.cpp / bandwidth.cpp
│       └── peer.hpp + address/wsa/crc32/socket_platform/wire_format
├── client/                      # ★ tight_client：设备端直连测试工具（交互式 CLI）
├── gateway/                     # 网关服务（namespace gateway）
│   ├── gateway_server.hpp       #   GatewayServer 骨架：分发器/定时器/tight 接入/JSON 编解码
│   ├── gateway_context.hpp      #   监听器共享上下文（服务句柄 + IO 钩子）
│   ├── hello/audio/device/forward_listener.hpp         # 4 个内置监听器
│   ├── gateway_reactor.hpp      #   Reactor（主循环 + IO/CPU 线程 + 定时器堆）
│   ├── gateway_channel.hpp      #   CopyChannel<T>（CSP 通道，深拷贝语义）
│   ├── gateway_blackboard.hpp   #   DataBoard 数据黑板
│   ├── gateway_session/auth/ratelimit/metrics*/        # 会话/鉴权/限流/指标
│   └── gateway_main.cpp         #   可执行入口（cloud_gateway）
├── tests/                       # 测试：框架 + 12 套件 + sim_device 模拟设备
├── docs/                        # 文档（架构 / API / 使用说明）
├── CMakeLists.txt
└── build.bat                    # Windows 一键构建（clean/debug/release/test）
```

---

## 3. 快速开始

### 3.1 构建与测试

```bat
rem Windows 一键构建（自动探测 cmake 与 MinGW g++）
build.bat              rem Release 构建
build.bat test         rem 构建并运行全部 12 个测试套件
build.bat clean debug  rem 清理 build/ 后 Debug 构建
```

等价 CMake 命令（Linux 去掉 `-G`/`-DCMAKE_CXX_COMPILER` 即可）：

```bash
cmake -B build -S . -G "MinGW Makefiles" -DCMAKE_CXX_COMPILER=g++
cmake --build build -j 4
ctest --test-dir build --output-on-failure
```

### 3.2 运行网关

```bash
./build/cloud_gateway.exe
```

默认（`gateway/gateway_main.cpp`）：tight UDP `0.0.0.0:9443`，token `gateway-shared-secret`，mtu 1400；指标 HTTP `:9090/metrics`；Ctrl+C 优雅退出。

### 3.3 设备直连测试工具（client/）

```bash
# 启动网关后，另开终端运行交互式设备客户端
./build/client/tight_client --host 127.0.0.1 --port 9443
# 交互命令: hello | ping | bye | send <文本> | audio [帧数] |
#           config <json> | cmd <文本> | peers | help | quit
```

### 3.4 tight 库独立使用

```cmake
# 在你的项目中直接引用 tight 库（自包含，零第三方依赖）
add_subdirectory(tight)
target_link_libraries(your_app PRIVATE tight)
```

```cpp
#include "tight/tight.hpp"   // 命名空间 tight::，详见 docs/api_reference.md
```

### 3.5 设备最小接入示例

```cpp
#include "tight/tight.hpp"

tight::TightConfig cfg;
cfg.bind  = tight::NetAddress("0.0.0.0", 0);
cfg.id    = "my-device";
cfg.token = "gateway-shared-secret";        // 必须与网关一致
// cfg.encryption_enabled 默认 true：握手自动完成 ECDH，数据自动加密

tight::TightTransport transport(cfg);
transport.set_message_callback([](const std::string& peer, tight::Bytes payload) {
    // 处理网关下行消息
});
transport.set_command_callback([](const std::string& peer, tight::Bytes payload) {
    // 处理保序命令（控制/按键）
});
transport.start();
transport.connect({"gateway-9443", tight::NetAddress("127.0.0.1", 9443)});

std::string hello = R"({"type":"hello","product_id":"p1","product_key":"k1",
                        "product_secret":"s1","device_name":"my-device"})";
transport.send("gateway-9443", tight::Bytes(hello.begin(), hello.end()));
transport.send_command("gateway-9443", {'c','m','d'});   // 命令通道
```

---

## 4. 测试套件（12 个，ctest 全绿）

| 套件 | 覆盖 |
|---|---|
| test_channel / test_blackboard / test_ratelimit / test_session / test_auth | 网关组件单元 |
| test_server | GatewayServer 生命周期 + listener 注册 |
| test_crypto | SHA-256 / X25519（RFC 7748 向量）/ AES-256（FIPS-197）/ AES-256-GCM（NIST 向量 + 篡改检测）/ HKDF |
| test_tight_pair | 双 TightTransport 真实 UDP 回环（数据 + 命令 + 加密开关两态） |
| test_tight_fec | RS 编解码（单/多擦除恢复、超能力失败）/ 熵冗余公式 / BBR 增益 / 慢包统计 / report 回环 / 测速列车聚合 |
| test_command | CommandChannel 保序、乱序挂起、3×RTT 超时跳过、重置 |
| test_e2e_final | 原生客户端 e2e（hello/ping/下行/鉴权失败/多设备/全流程） |
| test_e2e_sim | sim_device e2e（8 场景覆盖全部内置 listener 路径） |

---

## 5. 文档

| 文档 | 内容 |
|---|---|
| [docs/architecture.md](docs/architecture.md) | 架构说明：tight 协议内部机制与网关反应式架构 |
| [docs/api_reference.md](docs/api_reference.md) | 调用 API 参考：公共类、配置项、回调、JSON 协议 |
| [docs/usage_guide.md](docs/usage_guide.md) | 完整使用说明：构建、部署、设备接入、命令通道、扩展开发 |

许可：见 [LICENSE](LICENSE)。
