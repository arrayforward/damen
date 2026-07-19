# 架构说明

本文档描述 damen 的两层架构：**tight 传输协议**（可靠 UDP 传输层）与 **gateway 网关服务**（反应式业务层）。所有内容均与代码一一对应。

---

## 第一部分：tight 传输协议

### 1. 总体结构

```
creek/tight.hpp                 公共 API（TightTransport）
        │
tight/transport.cpp             TightTransport::Impl —— 核心调度
        ├── tight/reassembler.*   入向数据通路（重组 + RS 恢复 + 慢包统计）
        ├── tight/fragmenter.*    出向数据通路（分片 + RS 校验生成）
        ├── tight/report.*        链路报告（ACK 剪枝 + 丢包重传 + 带宽回传）
        ├── tight/command.*       命令通道（保序 + 乱序窗口）
        ├── tight/crypto.*        X25519 / SHA-256 / HKDF / AES-256-GCM
        ├── tight/fec.cpp         Reed-Solomon 擦除码（GF(2⁸)）
        ├── tight/bandwidth.cpp   BBR 带宽估算器
        ├── tight/packet_codec.cpp 线格式编解码 + CRC32
        └── tight/peer.hpp 等     Peer 状态、平台 socket、线格式常量
```

命名空间：公共 API 在 `creek`，内部实现细节在 `creek::tight_detail`。

### 2. 线程模型（4 线程）

`TightTransport::Impl` 在 `start()` 时启动 4 个线程（`tight/transport.cpp`）：

| 线程 | 职责 |
|---|---|
| reactor | 固定节拍（`flush_interval`，默认 10ms）：重发握手、发心跳、发 report、掉线检测（`check_offline`）、命令乱序窗口过期（`flush_commands`）、应用消息出队（`process_send_queue`） |
| receiver | 阻塞 `recvfrom` → `handle_packet` 分发（解密 → 按包型处理）。每包无日志，防止收包缓冲区溢出 |
| encode | 从 `m_encode_queue` 取消息，执行分片 + RS 编码 + 逐分片发包（CPU 密集与收包解耦） |
| sender | 从 `m_outbound_queue` 取已编码报文，令牌桶 pacing 后 `sendto`。限速不阻塞 reactor/encode |

锁纪律：`m_peers_mutex`（peer 表）→ `peer.m_mu`（peer 内部状态）固定顺序；回调（消息/命令/事件）先拷贝 `std::function` 再在锁外触发。

### 3. 线格式（48 字节头，`tight/packet_codec.cpp`）

```
偏移  长度  字段
0     4     magic = 0x54474854 ("TGHT")
4     1     version = 1
5     1     type（见下）
6     2     flags（数据报文=数据分片数；bit15=加密标志 kFlagEncrypted）
8     4     client_id（发送方随机 ID，区分方向）
12    8     session_id
20    4     sequence（可靠包序列号 / 命令序列号）
24    4     acknowledgment（ACK 确认的序列号）
28    4     message_id（消息分片组 ID）
32    2     fragment_index
34    2     fragment_count（数据分片数 + 校验分片数）
36    2     payload_size（线上负载长度，加密时含 16B 标签）
38    2     reserved（数据报文=分片真实长度）
40    4     tick（发送时刻 unix 毫秒低 32 位，对表与单向时延测量用）
44    4     checksum（CRC32/IEEE，对 头部(44 清零) + 负载 计算）
```

包型（`creek/types.hpp`）：

```
Handshake=0  HandshakeAck=1  Online=2  Heartbeat=3  Bye=4
Data=5  Parity=6  Ack=7  Report=8  Probe=9  Command=10
```

### 4. 连接建立与控制面

```
发起方                                响应方
  │ Handshake(role+id+token+ECDH公钥)   │
  │────────────────────────────────────>│ 校验 token → Established
  │ Handshake(role+id+token+ECDH公钥)   │（响应方也发自己的握手）
  │<────────────────────────────────────│
  │         HandshakeAck                │
  │<────────────────────────────────────│
  │ Online            →                 │ 双方 Online，互发 100KB 测速列车
```

- 握手为可靠包（带序列号，ACK 确认）；空 id 保留接入侧分配的 `anon-*` 身份（保证 token 校验与 ECDH 不中断）
- 心跳：每 `heartbeat`（默认 5s）携带本端 RTT 估计；`dead_timeout`（默认 30s）未收到任何报文则 Closed，`Node` 角色自动重连
- 下线：析构/`stop()` 广播 Bye

### 5. ECDH + AEAD（`tight/crypto.cpp`）

- **密钥交换**：握手负载尾部追加 32 字节 X25519 公钥。双方各自计算
  `shared = X25519(本地私钥, 对端公钥)`，低阶点（全零）则放弃加密
- **密钥派生**：`key = HKDF-SHA256(shared, salt = min(client_id)||max(client_id), "tight-data-key-v1")`，两端导出相同密钥
- **加密范围**：Data / Parity / Command 负载（控制包不加密），`flags`  bit15 标记密文
- **nonce（96 位，方向内唯一）**：
  - Data/Parity：`client_id(4) | message_id(4) | fragment_index(2) | type(1) | 0(1)`
  - Command：`client_id(4) | sequence(4) | type(1) | 0(3)`
- **AAD**：报文头前 44 字节（含加密标志与密文长度），密文与头部绑定
- **线上负载**：`密文 || 16B GCM 标签`；接收方先解密再分发，认证失败丢弃
- 重传报文用相同头字段重新加密（nonce 相同、明文相同，安全）
- 加密原语纯 C++ 实现：X25519（5×51 位 limb + `__int128`）、SHA-256、HKDF、AES-256（S 盒运行时生成）、GCM（GHASH），经 RFC 7748 / FIPS-197 / NIST GCM 向量验证

### 6. 可靠性与重传

- 可靠包（握手/数据分片）记录于 `peer.m_pending`；收到 Ack 按序列号移除并产生 RTT 样本
- 接收侧维护 `m_missing_seqs` 缺口表，每个 report 周期（默认 1s）把超时缺口（> 3.5×RTT，下限 100ms）列入丢包清单
- 发送侧 `Report::handle`：按 ACK 游标剪枝 pending，对清单内丢包立即重传（≤10 次）

### 7. Reed-Solomon FEC（`tight/fec.cpp`）

- 编码：Vandermonde 系数矩阵 `coef(i,j) = (j+1)^i`，第 i 个校验分片为全部数据分片的 GF(2⁸) 线性组合（i=0 时退化为 XOR）
- 解码：伴随式 + GF 高斯消元，任意 p 个校验分片恢复任意 p 个丢失分片
- **冗余率（信息论驱动，`Fragmenter::compute_parity_count_for`）**：
  `冗余率 = H(迟到率) × 1.2`，`H(p) = -p·log₂(p) - (1-p)·log₂(1-p)`，
  `parity = clamp(ceil(data_count × 冗余率), 1, 3)`。干净链路 H(0)=0 → 保底 1 片

### 8. 时钟对表与慢包统计（`tight/transport.cpp` `tight/peer.hpp`）

- **对表**：`offset = (对端tick − 本地到达) − RTT/2`，握手时建立（无 RTT 样本则挂起，首个 ACK 补算），**每次心跳重对表**（`offset = (offset×7 + 新样本)/8` 跟踪漂移）。偏移按 peer 存储，本地时间不变
- **单向传输时间**：`transit = (到达时刻 − 报文tick) + offset`（`transit_time_us`）
- **慢包**：`transit > late_rtt_multiplier × RTT`（默认 4 倍，可配）→ `m_late_samples / m_transit_samples` 计数
- **上报**：report offset 4 字段携带 `慢包率 × 10000`，每周期清零
- **消费**：对端收到后写入 `m_peer_late_ratio` → ① 驱动 RS 冗余率 ② 作为 BBR 辅助增益信号

### 9. RTT 与 BBR 带宽估算（`tight/bandwidth.cpp`）

- **RTT 三来源**：① ACK 往返（主）② 心跳单向传输 ×2 ③ report 单向传输 ×2（基于时钟偏移）
- **BtlBw**：投递率（ACK 字节数 / ACK RTT）的 10 样本窗口最大值；**RTprop**：历史最小 RTT
- **增益（pacing = BtlBw × gain，不低于下限）**：
  - 主信号 RTT 趋势：`RTT ≥ 1.5×RTprop`（或上升且 ≥1.2×）→ drain 0.75；`RTT ≤ 1.1×RTprop` 且未上升 → probe 1.25；否则 cruise 1.0
  - 辅助信号迟到率：≥10%（或上升且 ≥2%）→ 压至 0.75；>2% → 取消 probe；只下压不上调

### 10. 建连测速（`tight/transport.cpp` `tight/peer.hpp`）

- Online 后双方各发 `speed_test_bytes`（默认 100KB）空白 Probe 列车——**直发 socket，绕开令牌桶**（否则测到的是自身限速）
- 接收方按 20ms 间隙聚合列车：`带宽 = 总线上字节 / (末包到达 − 首包到达)`
- 下一 report 尾部 4 字节回传；发送方 `seed_bandwidth()` 设为 BtlBw 基线与下限
- `speed_test_enabled` 可整体关闭

### 11. 命令通道（`tight/command.cpp`）

- 用途：控制/按键信息；单报文（≤ mtu−48，超限 `send_command` 返回 false），不分片不重组
- 发送：跳过发送队列/reactor 节拍/编码线程，直入出站队列（插队）
- 保序：独立序列空间；按序立即上报 `CommandCallback`；乱序挂起**最多 3×RTT**（无 RTT 按 10ms），超时跳过缺口按序上报，缺口包后到直接丢弃；连续缺口各自重计窗口
- 会话重置：握手时双向重置序列状态

---

## 第二部分：gateway 网关服务

### 1. 反应式架构（`gateway/gateway_reactor.hpp` 等）

```
tight receiver 线程
   │ on_tight_message：JSON 解析 → GatewayMessage → m_inbox (CopyChannel)
   ▼
Reactor 主循环（10ms 节拍）
   │ swap_out 批量消息 → CPU 线程执行 process_message_batch
   ▼
process_one_message（观察者分发）
   │ 按消息类型调用注册的 listener（快照迭代，单 listener 异常隔离）
   ▼
listener 处理（纯状态操作 + m_pending_io 排队）
   ▼
execute_pending_io（批次末尾统一执行发包/下行回调）
   +
DataBoard.evolve_once（黑板演化，单层不级联）
```

组件：

- **CopyChannel**（`gateway_channel.hpp`）：CSP 风格通道，消息深拷贝，支持阻塞/非阻塞收发与 `swap_out` 批量取出
- **Reactor**（`gateway_reactor.hpp`）：主循环 + IO 线程 + CPU 线程 + 定时器最小堆；任务统计（慢任务 >10ms 计数）；可跳过定时器（延误 >1s 时跳过）
- **DataBoard**（`gateway_blackboard.hpp`）：设备/下游/指标三类条目，条目级互斥锁 + 脏标记 + 变更 key 集合 + 演化器注册
- **定时任务**：heartbeat_check（30s，2 倍心跳超时判离线）、session_cleanup（60s）、metrics_report（60s）

### 2. 监听器观察者模式（`gateway/gateway_server.hpp`）

- `add_message_listener(type, fn)`：按类型注册，多 listener 按序调用
- 写时复制注册、快照分发：listener 内可再注册，不死锁；音频帧高频路径零拷贝
- 内置监听器（独立文件，经 `GatewayContext` 访问服务，不反向依赖 Server）：

| 监听器 | 文件 | 消息类型 | 行为 |
|---|---|---|---|
| HelloListener | `gateway/hello_listener.hpp` | kDeviceHello | 连接限流 → 鉴权 → hello_ack/err + 下行 kDeviceHello |
| AudioListener | `gateway/audio_listener.hpp` | kDeviceAudioFrame / kDeviceAudioBoundary | 带宽限流 + 字节计费 + 下行转发 |
| DeviceListener | `gateway/device_listener.hpp` | kDeviceBye / kDevicePing / kInternalRateLimit | 会话关闭 / pong / 限流计数 |
| ForwardListener | `gateway/forward_listener.hpp` | kDeviceConfigUpdate / kDeviceFunctionCallOutput | 透传下行 |

- `GatewayContext`（`gateway/gateway_context.hpp`）：SessionManager / AuthService（start 时绑定）/ RateLimiter / Metrics + `m_schedule_io` / `m_send_to_device` / `m_downstream` 三个 IO 钩子

### 3. 设备消息协议（JSON，`gateway/gateway_server.hpp` 编解码）

上行（设备 → 网关，按子串识别 type）：`hello`（product_id/product_key/product_secret/device_name）、`config_update`、`function_call_output`、`audio_boundary`、`bye`、`ping`；无法识别的按 `audio_frame` 处理。

下行（网关 → 设备）：`hello_ack`、`hello_err`、`status`、`event`、`text`、`function_call`、`config_update_ack`、`config_update_err`、`audio_chunk`（原始负载透传）、`pong`。

### 4. 支撑组件

- **SessionManager**（`gateway_session.hpp`）：会话创建/激活/关闭、状态更新、字节与消息计数；会话 ID = `设备名-毫秒-指针`
- **AuthService**（`gateway_auth.hpp`）：可注入 validator；默认实现校验字段非空，返回 `dev-<name>` 与 24h 过期；另有 JWT 校验钩子
- **RateLimiter**（`gateway_ratelimit.hpp`）：令牌桶，默认四维度——connection（0.1/s，10s 窗口）、bandwidth（32KB/s）、audio_day（60min/天）、tokens_day（100K/天）；`configure()` 可改
- **Metrics**（`gateway_metrics.hpp`）：连接数、消息/字节进出、限流命中、认证成败计数；Prometheus 文本导出；`MetricsHttpServer` 默认 `:9090/metrics`
