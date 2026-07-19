#pragma once

#include "tight/types.hpp"

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace gateway {

using Bytes = std::vector<std::uint8_t>;

enum class MessageDirection : std::uint8_t {
    DeviceToGateway  = 0,
    GatewayToDevice  = 1,
    DownstreamIn     = 2,
    DownstreamOut    = 3,
};

enum class GatewayMessageType : std::uint16_t {
    kDeviceHello             = 0,
    kDeviceConfigUpdate      = 1,
    kDeviceFunctionCallOutput = 2,
    kDeviceAudioFrame        = 3,
    kDeviceAudioBoundary     = 4,
    kDeviceBye               = 5,
    kDevicePing              = 6,

    kToDeviceHelloAck        = 100,
    kToDeviceHelloErr        = 101,
    kToDeviceStatus          = 102,
    kToDeviceEvent           = 103,
    kToDeviceText            = 104,
    kToDeviceFunctionCall    = 105,
    kToDeviceConfigAck       = 106,
    kToDeviceConfigErr       = 107,
    kToDeviceAudioChunk      = 108,
    kToDevicePong            = 109,

    kInternalSessionCreated  = 200,
    kInternalSessionClosed   = 201,
    kInternalRateLimit       = 202,
    kInternalAuthResult      = 203,
    kInternalHeartbeatTick   = 204,
};

constexpr const char* kGatewayMessageTypeNames[] = {
    "device_hello",              // 0
    "config_update",             // 1
    "function_call_output",      // 2
    "audio_frame",               // 3
    "audio_boundary",            // 4
    "bye",                       // 5
    "ping",                      // 6
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
    "hello_ack",                 // 100
    "hello_err",                 // 101
    "status",                    // 102
    "event",                     // 103
    "text",                      // 104
    "function_call",             // 105
    "config_update_ack",         // 106
    "config_update_err",         // 107
    "audio_chunk",               // 108
    "pong",                      // 109
};

enum class ConvaiStatus : std::uint8_t {
    kIdle            = 0,
    kListening       = 1,
    kThinking        = 2,
    kAnswering       = 3,
    kInterrupted     = 4,
    kAnswerFinished  = 5,
};

enum class ConvaiEvent : std::uint8_t {
    kConnected    = 0,
    kDisconnected = 1,
    kFailed       = 2,
    kUpdated      = 3,
};

struct GatewayMessage {
    GatewayMessageType    m_type{GatewayMessageType::kDeviceHello};
    MessageDirection      m_direction{MessageDirection::DeviceToGateway};
    std::string           m_device_id;
    std::string           m_session_id;
    Bytes                 m_payload;
    std::uint64_t         m_timestamp_ms{};
    std::uint32_t         m_sequence{};

    GatewayMessage() = default;

    GatewayMessage(GatewayMessageType t, MessageDirection d, std::string dev_id,
                   std::string sess_id, Bytes payload, std::uint64_t ts)
        : m_type(t), m_direction(d), m_device_id(std::move(dev_id)),
          m_session_id(std::move(sess_id)), m_payload(std::move(payload)),
          m_timestamp_ms(ts) {}

    GatewayMessage(const GatewayMessage&) = default;
    GatewayMessage& operator=(const GatewayMessage&) = default;
    GatewayMessage(GatewayMessage&&) = default;
    GatewayMessage& operator=(GatewayMessage&&) = default;
};

struct ChangeSet {
    std::vector<GatewayMessage> m_new_messages;
    std::vector<std::pair<std::string, std::string>> m_blackboard_changes;
    std::vector<std::function<void()>> m_pending_io_calls;
};

struct AuthResult {
    bool           m_ok{false};
    std::string    m_device_id;
    std::string    m_session_id;
    std::string    m_jwt;
    std::string    m_error_code;
    std::string    m_error_message;
    std::uint64_t  m_expires_at{};
};

struct DownstreamConfig {
    std::string m_session_id;
    std::string m_device_id;
    std::string m_jwt;
    std::string m_audio_codec{"g711a"};
    std::uint32_t m_sample_rate{8000};
};

using DownstreamMsgCallback = std::function<void(const GatewayMessage& msg)>;
using DownstreamErrCallback = std::function<void(const std::string& session_id,
                                                   const std::string& error)>;

struct RateLimitResult {
    bool            m_allowed{true};
    std::string     m_dimension;
    std::string     m_error_code;
    std::uint64_t   m_remaining{};
};

// Minimal JSON string-field extraction shared by the gateway and its
// listeners (the payloads handled here are flat, trusted JSON documents).
inline std::string extract_json_field(const std::string& json,
                                      const std::string& key) {
    std::string search = "\"" + key + "\":\"";
    auto pos = json.find(search);
    if (pos == std::string::npos) {
        search = "\"" + key + "\": \"";
        pos = json.find(search);
    }
    if (pos == std::string::npos) return "";
    pos += search.size();
    auto end = json.find('"', pos);
    if (end == std::string::npos) return "";
    return json.substr(pos, end - pos);
}

} // namespace gateway
