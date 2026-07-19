# API 参考

本文档列出 damen 的全部公开调用接口，与头文件一一对应。

---

## 1. tight 传输层（`namespace tight`）

包含方式：`#include "tight/tight.hpp"`（聚合 `creek/types.hpp`、`creek/tight/packet_codec.hpp`、`creek/tight/fec.hpp`、`creek/tight/bandwidth.hpp`）。

### 1.1 TightTransport（`creek/tight.hpp`）

可靠 UDP 传输端点。不可拷贝；析构自动广播 Bye 并停止。

```cpp
class TightTransport {
public:
    using MessageCallback = std::function<void(const std::string& peer_id, Bytes payload)>;
    using PeerCallback    = std::function<void(const PeerEvent& event)>;
    using CommandCallback = std::function<void(const std::string& peer_id, Bytes payload)>;

    explicit TightTransport(TightConfig config);
    ~TightTransport();

    // 注册回调（任意线程可设，触发于内部 receiver/reactor 线程）
    void set_message_callback(MessageCallback callback);   // 数据消息（可靠、已重组）
    void set_peer_callback(PeerCallback callback);         // 链路状态事件
    void set_command_callback(CommandCallback callback);   // 保序命令

    bool start();                       // 绑定端口并启动 4 个内部线程
    void stop();                        // 广播 Bye 并停止（幂等）

    bool connect(const RemotePeer& remote);               // 主动连接（握手）
    bool send(const std::string& peer_id, Bytes payload);          // 优先级 0
    bool send_priority(const std::string& peer_id, Bytes payload, int priority);
    bool send_command(const std::string& peer_id, Bytes payload);  // ≤ mtu-48，超限返回 false

    std::vector<PeerEvent> peers() const;   // 当前 peer 快照
    std::uint16_t local_port() const;       // 实际绑定端口
};
```

- `send` / `send_priority`：消息入队，reactor 按优先级（大者先）出队发送；仅 Online 状态发送，否则每拍重试；队列满（`queue_limit`）返回 false
- 回调内禁止调用会长时间阻塞的 API；`send*` 系列可在回调内安全调用

### 1.2 基础类型（`creek/types.hpp`）

```cpp
using Bytes = std::vector<std::uint8_t>;

enum class PacketType : std::uint8_t {
    Handshake=0, HandshakeAck=1, Online=2, Heartbeat=3, Bye=4,
    Data=5, Parity=6, Ack=7, Report=8, Probe=9, Command=10,
};

struct PacketHeader { /* magic/version/type/flags/client_id/session_id/
                         sequence/acknowledgment/message_id/fragment_index/
                         fragment_count/payload_size/reserved/tick/checksum */ };

enum class LinkRole  : std::uint8_t { Leaf = 0, Node = 1 };
enum class LinkState : std::uint8_t { Closed=0, Handshake=1, Established=2, Online=3 };

struct PeerEvent {
    std::string   id;          // 对端 id（匿名接入为 anon-*）
    LinkRole      role;
    LinkState     state;
    std::uint32_t client_id;
};

struct NetAddress { std::string host; std::uint16_t port; };
struct RemotePeer { std::string id; NetAddress address; };

std::uint64_t unix_millis();   // 当前 Unix 毫秒
```

### 1.3 TightConfig（`creek/types.hpp`）

| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `bind` | NetAddress | — | 本地绑定地址（端口 0 = 系统分配） |
| `id` | std::string | — | 本端标识（空 id 由对端分配 anon 身份） |
| `token` | std::string | — | 接入令牌，握手校验，两端必须一致 |
| `role` | LinkRole | Leaf | Node 掉线后自动重连 |
| `mtu` | std::size_t | 1200 | 单个报文最大字节（含 48 字节头） |
| `heartbeat` | milliseconds | 5s | 心跳间隔（兼作时钟对表节拍） |
| `report_interval` | milliseconds | 1s | 链路报告周期（慢包率/丢包/测速带宽） |
| `flush_interval` | milliseconds | 10ms | reactor 节拍 |
| `dead_timeout` | milliseconds | 30s | 无报文判离线时长 |
| `retransmit_timeout` | milliseconds | 500ms | 重连时的握手重发间隔 |
| `initial_bandwidth_bytes` | std::uint64_t | 100MB/s | 初始带宽估计与下限（测速后更新） |
| `queue_limit` | std::size_t | 65536 | 应用消息排队上限 |
| `late_rtt_multiplier` | double | 4.0 | 慢包判定：传输时间 > 倍数 × RTT |
| `speed_test_enabled` | bool | true | 建连测速开关 |
| `speed_test_bytes` | std::size_t | 100KB | 测速列车大小 |
| `encryption_enabled` | bool | true | ECDH + AES-256-GCM 开关 |

### 1.4 PacketCodec（`creek/tight/packet_codec.hpp`）

```cpp
class PacketCodec {
public:
    static Bytes encode(const PacketHeader& header, const Bytes& payload);
    static bool decode(const Bytes& datagram, PacketHeader& header, Bytes& payload);
    static std::uint32_t crc32(const std::uint8_t* data, std::size_t size);
};
```

### 1.5 ReedSolomon（`creek/tight/fec.hpp`）

GF(2⁸) 擦除码；p 个校验分片可恢复任意 p 个丢失分片。

```cpp
class ReedSolomon {
public:
    static std::vector<Bytes> encode(const std::vector<Bytes>& data,
                                     std::size_t parity_count, std::size_t width);
    static bool decode(std::vector<std::optional<Bytes>>& data,
                       const std::vector<std::pair<std::size_t, Bytes>>& parity,
                       std::size_t width);
};
```

### 1.6 BandwidthEstimator（`creek/tight/bandwidth.hpp`）

```cpp
class BandwidthEstimator {
public:
    explicit BandwidthEstimator(std::uint64_t initial_bytes_per_second);
    void on_ack(std::size_t bytes, std::chrono::microseconds rtt);  // bytes==0 为纯 RTT 样本
    void on_late_ratio(double late_ratio);                          // 辅助增益信号
    void seed_bandwidth(std::uint64_t bytes_per_second);            // 外部实测注入基线
    std::uint64_t bytes_per_second() const;
    std::chrono::microseconds rtt() const;
};
```

### 1.7 BlockingQueue / Logger（`creek/blocking_queue.hpp` `creek/logger.hpp`）

```cpp
template <typename T> class BlockingQueue {
    explicit BlockingQueue(std::size_t capacity = 0);   // 0 = 无界
    bool push(T item);            // 满则阻塞
    bool try_push(T item);
    std::optional<T> take();
    std::optional<T> take_for(std::chrono::milliseconds timeout);
    std::optional<T> poll();
    void close();  bool is_closed() const;  std::size_t size() const;
};

// 日志：TIGHT_LOG_DEBUG/INFO/WARN/ERROR(msg)
tight::Logger::instance().set_level(tight::LogLevel::Debug);
```

---

## 2. 网关服务（`namespace gateway`）

### 2.1 GatewayServer（`gateway/gateway_server.hpp`）

```cpp
class GatewayServer {
public:
    explicit GatewayServer(GatewayConfig config);
    ~GatewayServer();   // 自动 stop()

    void set_downstream_callback(DownstreamMsgCallback cb);        // 下行消息钩子
    void set_downstream_error_callback(DownstreamErrCallback cb);  // 下行错误钩子

    // 监听器注册（观察者模式）：多 listener 按序调用，运行时可再注册
    using MessageListener = std::function<void(const GatewayMessage& msg)>;
    void add_message_listener(GatewayMessageType type, MessageListener listener);

    std::shared_ptr<DataBoard>   board();
    std::shared_ptr<Metrics>     metrics();
    std::shared_ptr<Reactor>     reactor();
    std::shared_ptr<RateLimiter> ratelimiter();

    bool start();                 // 启动 tight 传输 + reactor + 定时器
    void stop();                  // 幂等

    bool send_to_device(const std::string& device_id, const GatewayMessage& msg);
};
```

### 2.2 GatewayMessage 与枚举（`gateway/gateway_types.hpp`）

```cpp
struct GatewayMessage {
    GatewayMessageType m_type;
    MessageDirection   m_direction;   // DeviceToGateway/GatewayToDevice/DownstreamIn/DownstreamOut
    std::string        m_device_id;
    std::string        m_session_id;
    Bytes              m_payload;
    std::uint64_t      m_timestamp_ms;
    std::uint32_t      m_sequence;
};

enum class GatewayMessageType : std::uint16_t {
    kDeviceHello=0, kDeviceConfigUpdate=1, kDeviceFunctionCallOutput=2,
    kDeviceAudioFrame=3, kDeviceAudioBoundary=4, kDeviceBye=5, kDevicePing=6,
    kToDeviceHelloAck=100, kToDeviceHelloErr=101, kToDeviceStatus=102,
    kToDeviceEvent=103, kToDeviceText=104, kToDeviceFunctionCall=105,
    kToDeviceConfigAck=106, kToDeviceConfigErr=107, kToDeviceAudioChunk=108,
    kToDevicePong=109,
    kInternalSessionCreated=200, kInternalSessionClosed=201,
    kInternalRateLimit=202, kInternalAuthResult=203, kInternalHeartbeatTick=204,
};

// 辅助：从扁平 JSON 提取字符串字段
std::string extract_json_field(const std::string& json, const std::string& key);
```

### 2.3 内置监听器与上下文

```cpp
// gateway/gateway_context.hpp
struct GatewayContext {
    std::shared_ptr<SessionManager> m_session;
    std::shared_ptr<AuthService>    m_auth;         // start() 时绑定
    std::shared_ptr<RateLimiter>    m_ratelimiter;
    std::shared_ptr<Metrics>        m_metrics;
    std::function<void(std::function<void()>)>                 m_schedule_io;
    std::function<bool(const std::string&, const GatewayMessage&)> m_send_to_device;
    std::function<void(const GatewayMessage&)>                 m_downstream;
};
using ListenerRegistration = std::pair<GatewayMessageType, MessageListener>;

// 各监听器构造后通过 listeners() 提供注册表，由 GatewayServer 统一注册
HelloListener(ctx)->listeners();    // kDeviceHello
AudioListener(ctx)->listeners();    // kDeviceAudioFrame / kDeviceAudioBoundary
DeviceListener(ctx)->listeners();   // kDeviceBye / kDevicePing / kInternalRateLimit
ForwardListener(ctx)->listeners();  // kDeviceConfigUpdate / kDeviceFunctionCallOutput
```

自定义监听器无需继承任何基类，直接：

```cpp
server.add_message_listener(GatewayMessageType::kDevicePing,
    [](const gateway::GatewayMessage& msg) { /* ... */ });
```

### 2.4 GatewayConfig（`gateway/gateway_config.hpp`）

| 字段 | 默认值 |
|---|---|
| `m_tight.host / port / id / token / mtu / heartbeat / dead_timeout` | `0.0.0.0` / `9443` / 空 / `gateway-token` / `1200` / `5s` / `30s` |
| `m_downstream.target / connect_timeout / reconnect_delay` | `media-service:50051` / `10s` / `5s` |
| `m_redis.url / timeout` | `redis://redis:6379` / `200ms` |
| `m_server.max_connections / heartbeat_seconds / max_payload_bytes / session_timeout` | `5000` / `60` / `65536` / `60min` |
| `m_ratelimit.connection_cooldown / max_bandwidth_bytes_per_sec / max_audio_per_day / max_llm_tokens_per_day` | `10s` / `32KB` / `60min` / `100000` |
| `m_metrics.port / path` | `9090` / `/metrics` |
| `m_log_level` | `info` |

### 2.5 支撑组件

```cpp
// SessionManager（gateway_session.hpp）
std::string create_session(const std::string& device_id);
bool activate_session(const std::string& device_id, const std::string& session_id);
void close_session(const std::string& device_id);
void update_status(const std::string& device_id, ConvaiStatus status);
void record_bytes_in/out(const std::string& device_id, std::uint64_t bytes);
bool is_connected(const std::string& device_id);
std::optional<DataBoard::DeviceState> device_state(const std::string& device_id);

// AuthService（gateway_auth.hpp）：构造注入 validator；默认宽松通过
void authenticate(product_id, product_key, product_secret, device_name, AuthCallback);
void set_jwt_validator(std::function<bool(const std::string&)>);

// RateLimiter（gateway_ratelimit.hpp）
RateLimitResult check(const std::string& device_id, const std::string& dimension,
                      std::uint64_t cost = 1);
void configure(const std::string& dimension, BucketConfig);
// 默认维度：connection(0.1/s) bandwidth(32KB/s) audio_day(60min) tokens_day(100K)

// Metrics（gateway_metrics.hpp）
on_connection_open/close, on_message_in/out, on_bytes_in/out,
on_ratelimit_hit, on_auth_success/failure;
std::string prometheus_text();   // Prometheus 文本导出
void log_snapshot();

// MetricsHttpServer（gateway_metrics_http.hpp）
MetricsHttpServer(const GatewayConfig&, std::shared_ptr<Metrics>);
bool start();  void stop();      // GET /metrics → Prometheus 文本
```

### 2.6 DataBoard / Reactor / CopyChannel

```cpp
// DataBoard（gateway_blackboard.hpp）
BlackboardEntry<DeviceState>* device(const std::string& device_id);
BlackboardEntry<DownstreamState>* downstream(const std::string& session_id);
void mark_changed(const std::string& key);
std::unordered_set<std::string> take_changed_keys();
void register_evolver(const std::string& name, std::function<void(DataBoard*)>);
void evolve_once();                       // 单层演化
std::unordered_map<std::string, DeviceState> device_snapshot();
MetricsState metrics_snapshot();

// Reactor（gateway_reactor.hpp）
void set_message_processor(std::function<void(std::vector<GatewayMessage>&)>);
void set_data_evolver(std::function<void()>);
bool submit_io(Task);  bool submit_cpu(Task);
bool schedule_timer(const std::string& name, std::function<void()>,
                    std::chrono::milliseconds interval, bool skippable = true);
std::vector<TaskStats> task_stats() const;

// CopyChannel（gateway_channel.hpp）：send/try_send/recv/try_recv/swap_out/close
```

---

## 3. 测试辅助：SimDevice（`tests/sim_device.hpp`）

```cpp
SimDevice(const std::string& device_name, std::uint16_t gateway_port,
          std::uint16_t local_port = 0);   // local_port=0 → gateway_port+1
bool start();  void stop();
bool send_hello(product_id="test-pid", product_key="test-pkey", product_secret="test-psec");
bool send_ping();  bool send_bye();
bool send_config_update(const std::string& config_json);
bool send_audio_frame(const std::vector<std::uint8_t>& g711);
bool send_audio_boundary(const std::string& event_type);
bool send_function_call_output(const std::string& call_id, const std::string& output);
bool send_json(const std::string& json);
bool wait_for_connected(timeout = 5s);
bool wait_for_json_containing(const std::string& substr, timeout = 5s);
bool has_received_containing(const std::string& substr);
std::vector<RecvEntry> drain_received();
```
