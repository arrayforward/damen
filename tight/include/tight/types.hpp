#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

namespace tight {

using Bytes = std::vector<std::uint8_t>;

enum class PacketType : std::uint8_t {
    Handshake    = 0,
    HandshakeAck = 1,
    Online       = 2,
    Heartbeat    = 3,
    Bye          = 4,
    Data         = 5,
    Parity       = 6,
    Ack          = 7,
    Report       = 8,
    Probe        = 9,
    Command      = 10,
};

struct PacketHeader {
    std::uint32_t magic{};
    std::uint8_t  version{};
    PacketType    type{};
    std::uint16_t flags{};
    std::uint32_t client_id{};
    std::uint64_t session_id{};
    std::uint32_t sequence{};
    std::uint32_t acknowledgment{};
    std::uint32_t message_id{};
    std::uint16_t fragment_index{};
    std::uint16_t fragment_count{};
    std::uint16_t payload_size{};
    std::uint16_t reserved{};
    std::uint32_t tick{};
    std::uint32_t checksum{};
};

enum class LinkRole : std::uint8_t {
    Leaf = 0,
    Node = 1,
};

enum class LinkState : std::uint8_t {
    Closed      = 0,
    Handshake   = 1,
    Established = 2,
    Online      = 3,
};

struct PeerEvent {
    std::string  id;
    LinkRole     role{LinkRole::Leaf};
    LinkState    state{LinkState::Closed};
    std::uint32_t client_id{};
};

struct NetAddress {
    std::string  host;
    std::uint16_t port{};

    NetAddress() = default;
    NetAddress(std::string h, std::uint16_t p)
        : host(std::move(h)), port(p) {}
};

struct RemotePeer {
    std::string id;
    NetAddress address;
};

struct TightConfig {
    NetAddress    bind;
    std::string   id;
    std::string   token;
    LinkRole      role{LinkRole::Leaf};
    std::size_t    mtu{1200};
    std::chrono::milliseconds heartbeat{std::chrono::seconds(5)};
    std::chrono::milliseconds report_interval{std::chrono::seconds(1)};
    std::chrono::milliseconds flush_interval{std::chrono::milliseconds(10)};
    std::chrono::milliseconds dead_timeout{std::chrono::seconds(30)};
    std::chrono::milliseconds retransmit_timeout{std::chrono::milliseconds(500)};
    std::uint64_t  initial_bandwidth_bytes{100 * 1024 * 1024};
    std::size_t    queue_limit{65536};
    // A data packet is "late" when its transit time (one-way, computed with
    // the per-peer clock offset) exceeds late_rtt_multiplier * RTT.
    double           late_rtt_multiplier{4.0};
    // After a link comes Online each end sends a speed-test train of blank
    // Probe datagrams; the receiver estimates the inbound bandwidth from the
    // packet arrival times and reports it back. Disable to skip probing.
    bool             speed_test_enabled{true};
    std::size_t      speed_test_bytes{100 * 1024};
    // ECDH + AEAD：握手阶段交换 X25519 公钥并 HKDF 派生会话密钥，
    // 数据分组（Data/Parity/Command）使用 AES-256-GCM 加密
    bool             encryption_enabled{true};
    // 资源占用：socket 内核缓冲（SO_RCVBUF/SO_SNDBUF 各自大小）与内部队列容量
    std::size_t    socket_buffer_bytes{8 * 1024 * 1024};
    std::size_t    encode_queue_limit{4096};
    std::size_t    outbound_queue_limit{65536};
    // 客户端精简模式：单线程（receiver/encode/sender 职责全部由 reactor
    // 节拍合并）、线程使用 64KB 小栈、内核缓冲与队列上限自动收紧
    // （socket_buffer≤16KB、encode≤256、outbound≤1024、queue_limit≤1024），
    // 面向嵌入式/单连接客户端；可经 set_lite_mode() 运行时动态切换
    bool             lite_mode{false};
};

inline std::uint64_t unix_millis() {
    using namespace std::chrono;
    auto now = system_clock::now();
    return static_cast<std::uint64_t>(
        duration_cast<milliseconds>(now.time_since_epoch()).count());
}

} // namespace tight
