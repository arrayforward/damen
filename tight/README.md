# tight —— 可靠 UDP 传输协议库

tight 是一个自包含、零第三方依赖的 C++17 可靠 UDP 传输库，面向端云实时通信场景（IoT 设备 ↔ 云端网关）。

## 特性

- **可靠传输**：ACK 确认 + NACK 丢包重传（≤10 次），心跳保活，掉线自动重连
- **ECDH + AEAD**：握手 X25519 密钥交换（RFC 7748），HKDF-SHA256 派生会话密钥，数据分组 AES-256-GCM 加密（报文头 AAD 绑定），可配置开关
- **Reed-Solomon FEC**：GF(2⁸) 擦除码，p 个校验分片恢复任意 p 个丢失分片；冗余率由迟到率信息熵 H(p) × 1.2 动态驱动
- **BBR 拥塞控制**：BtlBw 窗口最大值 + RTprop 最小 RTT；RTT 趋势为主、迟到率为辅的增益控制，令牌桶 pacing
- **建连测速**：Online 后互发 100KB 探测列车，按到达时间测量带宽（可开关）
- **时钟对表**：握手对表（补偿 RTT/2）+ 每次心跳重对表，单向传输时间可测
- **重传可协商**：`retransmit_enabled` 开关经握手能力标志通告，任一端可单方面关闭链路重传（纯 FEC 兜底），在途内存从 ∝码率 降为常数 ~24KB
- **命令通道**：单报文控制指令，保序投递，乱序最多等待 3×RTT，插队直发
- **四线程模型**：reactor / receiver / encode / sender 分离

## 目录

```
tight/
├── CMakeLists.txt          # 库构建（独立 / add_subdirectory 两用）
├── include/tight/          # 公共接口（对外 API）
│   ├── tight.hpp           #   TightTransport 主接口（聚合头）
│   ├── types.hpp           #   基础类型 + TightConfig
│   ├── packet_codec.hpp    #   线格式编解码 + CRC32
│   ├── fec.hpp             #   ReedSolomon 擦除码
│   ├── bandwidth.hpp       #   BBR 带宽估算器
│   ├── blocking_queue.hpp  #   有界阻塞队列
│   └── logger.hpp          #   日志（TIGHT_LOG_* 宏）
└── src/                    # 私有实现（不对外暴露，namespace tight::tight_detail）
```

## 文档

完整使用文档（含客户端/服务器端示例、配置参考、行为约定）：[docs/usage.md](docs/usage.md)

## 使用

### 方式一：add_subdirectory（推荐）

```cmake
add_subdirectory(tight)
target_link_libraries(your_app PRIVATE tight)
```

### 方式二：安装后链接

```bash
cmake -B build -S tight && cmake --build build
cmake --install build --prefix /your/prefix
# 头文件在 prefix/include/tight/，库在 prefix/lib/libtight.a
```

### 最小示例

```cpp
#include "tight/tight.hpp"

tight::TightConfig cfg;
cfg.bind  = tight::NetAddress("0.0.0.0", 0);
cfg.id    = "my-endpoint";
cfg.token = "shared-secret";
// cfg.encryption_enabled 默认 true：握手自动 ECDH，数据自动 AES-256-GCM 加密

tight::TightTransport t(cfg);
t.set_message_callback([](const std::string& peer, tight::Bytes payload) { /* ... */ });
t.set_command_callback([](const std::string& peer, tight::Bytes payload) { /* ... */ });
t.set_peer_callback([](const tight::PeerEvent& ev) { /* ... */ });

t.start();
t.connect({"server", tight::NetAddress("127.0.0.1", 9443)});
t.send("server", {'h','i'});
t.send_command("server", {'c','m','d'});   // 命令通道（单报文、保序、插队）
t.stop();
```

### 关键配置（`tight::TightConfig`）

| 字段 | 默认 | 说明 |
|---|---|---|
| `mtu` | 1200 | 单报文最大字节（含 48 字节头） |
| `encryption_enabled` | true | ECDH + AES-256-GCM 开关 |
| `speed_test_enabled` / `speed_test_bytes` | true / 100KB | 建连测速 |
| `late_rtt_multiplier` | 4.0 | 慢包阈值（传输时间 > 倍数 × RTT） |
| `heartbeat` / `dead_timeout` | 5s / 30s | 保活与掉线检测 |
| `initial_bandwidth_bytes` | 100MB/s | 初始限速（测速后自动校准） |
| `lite_mode` | false | 客户端精简模式：单线程 + 64KB 小栈 + 小缓冲（空闲实例约 76KB），`set_lite_mode()` 可运行时切换 |
| `socket_buffer_bytes` | 8MB | 内核收/发缓冲各自大小（lite ≤16KB） |

完整 API 见 [../docs/api_reference.md](../docs/api_reference.md)，架构细节见 [../docs/architecture.md](../docs/architecture.md)。
