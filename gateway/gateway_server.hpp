#pragma once

#include "tight/tight.hpp"
#include "gateway/gateway_config.hpp"
#include "gateway/gateway_types.hpp"
#include "gateway/gateway_channel.hpp"
#include "gateway/gateway_blackboard.hpp"
#include "gateway/gateway_reactor.hpp"
#include "gateway/gateway_session.hpp"
#include "gateway/gateway_auth.hpp"
#include "gateway/gateway_ratelimit.hpp"
#include "gateway/gateway_metrics.hpp"
#include "gateway/gateway_context.hpp"
#include "gateway/hello_listener.hpp"
#include "gateway/audio_listener.hpp"
#include "gateway/device_listener.hpp"
#include "gateway/forward_listener.hpp"
#include "tight/logger.hpp"

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace gateway {

class GatewayServer {
public:
    explicit GatewayServer(GatewayConfig config)
        : m_config(std::move(config))
        , m_board(std::make_shared<DataBoard>())
        , m_inbox(std::make_shared<CopyChannel<GatewayMessage>>(32768))
        , m_session(std::make_shared<SessionManager>(m_board))
        , m_ratelimiter(std::make_shared<RateLimiter>())
        , m_metrics(std::make_shared<Metrics>(m_board))
        , m_reactor(std::make_shared<Reactor>(m_inbox)) {
        register_builtin_listeners();
    }

    ~GatewayServer() { stop(); }

    GatewayServer(const GatewayServer&) = delete;
    GatewayServer& operator=(const GatewayServer&) = delete;

    void set_downstream_callback(DownstreamMsgCallback cb) {
        m_downstream_cb = std::move(cb);
    }

    void set_downstream_error_callback(DownstreamErrCallback cb) {
        m_downstream_err_cb = std::move(cb);
    }

    // Registers a listener for a message type (observer pattern). Multiple
    // listeners per type are invoked in registration order. Registration is
    // thread-safe (copy-on-write); dispatch iterates a snapshot, so a
    // listener may itself register further listeners without deadlocking.
    void add_message_listener(GatewayMessageType type, MessageListener listener) {
        std::lock_guard<std::mutex> lock(m_listeners_mutex);
        auto& slot = m_listeners[static_cast<int>(type)];
        auto next = std::make_shared<ListenerList>(slot ? *slot : ListenerList{});
        next->push_back(std::move(listener));
        slot = std::move(next);
    }

    std::shared_ptr<DataBoard> board() { return m_board; }
    std::shared_ptr<Metrics> metrics() { return m_metrics; }
    std::shared_ptr<Reactor> reactor() { return m_reactor; }
    std::shared_ptr<RateLimiter> ratelimiter() { return m_ratelimiter; }

    bool start() {
        if (m_running.load()) return true;
        TIGHT_LOG_INFO("GatewayServer starting...");

        m_auth = std::make_shared<AuthService>(
            [this](const std::string& pid, const std::string& pkey,
                   const std::string& psec, const std::string& dname) -> AuthResult {
                return do_authenticate(pid, pkey, psec, dname);
            });
        // The auth service exists only after start(); bind it into the
        // listener context now.
        m_ctx->m_auth = m_auth;

        tight::TightConfig tc;
        tc.bind = tight::NetAddress(
            m_config.m_tight.host,
            m_config.m_tight.port);
        tc.id = m_config.m_tight.id.empty()
            ? ("gateway-" + std::to_string(m_config.m_tight.port))
            : m_config.m_tight.id;
        tc.token = m_config.m_tight.token;
        tc.role = tight::LinkRole::Node;
        tc.mtu = m_config.m_tight.mtu;
        tc.heartbeat = m_config.m_tight.heartbeat;
        tc.dead_timeout = m_config.m_tight.dead_timeout;
        tc.queue_limit = m_config.m_server.max_payload_bytes;

        m_transport = std::make_unique<tight::TightTransport>(tc);
        m_transport->set_message_callback(
            [this](const std::string& peer_id, tight::Bytes payload) {
                on_tight_message(peer_id, std::move(payload));
            });
        m_transport->set_peer_callback(
            [this](const tight::PeerEvent& event) {
                on_tight_peer_event(event);
            });

        if (!m_transport->start()) {
            TIGHT_LOG_ERROR("Failed to start TightTransport");
            return false;
        }
        TIGHT_LOG_INFO("TightTransport started on port " +
                       std::to_string(m_transport->local_port()));

        m_reactor->set_message_processor(
            [this](std::vector<GatewayMessage>& batch) {
                process_message_batch(batch);
            });
        m_reactor->set_data_evolver([this]() {
            m_board->evolve_once();
        });

        m_reactor->schedule_timer("heartbeat_check",
            [this]() { heartbeat_check(); },
            std::chrono::seconds(30), true);

        m_reactor->schedule_timer("session_cleanup",
            [this]() { session_cleanup(); },
            std::chrono::seconds(60), true);

        m_reactor->schedule_timer("metrics_report",
            [this]() { m_metrics->log_snapshot(); },
            std::chrono::seconds(60), true);

        m_reactor->start();
        m_running.store(true);
        TIGHT_LOG_INFO("GatewayServer started successfully");
        return true;
    }

    void stop() {
        if (!m_running.exchange(false)) return;
        TIGHT_LOG_INFO("GatewayServer stopping...");
        m_reactor->stop();
        m_inbox->close();
        if (m_transport) {
            m_transport->stop();
        }
        TIGHT_LOG_INFO("GatewayServer stopped");
    }

    bool send_to_device(const std::string& device_id, const GatewayMessage& msg) {
        if (!m_transport) return false;

        Bytes wire = serialize_downstream(msg);
        if (wire.empty()) {
            return false;
        }

        auto* entry = m_board->device(device_id);
        std::string peer_id;
        {
            std::lock_guard<std::mutex> lock(entry->m_mutex);
            peer_id = entry->m_data.m_peer_id;
        }
        if (peer_id.empty()) {
            peer_id = device_id;
        }
        bool ok = m_transport->send_priority(peer_id, std::move(wire), 0);
        if (ok) {
            m_metrics->on_message_out(device_id, "device");
            m_metrics->on_bytes_out(wire.size());
        }
        return ok;
    }

private:
    Bytes serialize_downstream(const GatewayMessage& msg) {
        std::string json = format_downstream_json(msg);
        if (json.empty()) return {};
        return Bytes(json.begin(), json.end());
    }

    std::string format_downstream_json(const GatewayMessage& msg) {
        switch (msg.m_type) {
        case GatewayMessageType::kToDeviceHelloAck: {
            return "{\"type\":\"hello_ack\",\"device_id\":\"" +
                   msg.m_device_id + "\",\"session_id\":\"" +
                   msg.m_session_id + "\"}";
        }
        case GatewayMessageType::kToDeviceHelloErr: {
            auto payload_str = std::string(
                reinterpret_cast<const char*>(msg.m_payload.data()),
                msg.m_payload.size());
            return "{\"type\":\"hello_err\",\"device_id\":\"" +
                   msg.m_device_id + "\",\"body\":{\"code\":\"" +
                   payload_str + "\"}}";
        }
        case GatewayMessageType::kToDeviceStatus: {
            auto payload_str = std::string(
                reinterpret_cast<const char*>(msg.m_payload.data()),
                msg.m_payload.size());
            return "{\"type\":\"status\",\"status\":\"" +
                   payload_str + "\",\"session_id\":\"" +
                   msg.m_session_id + "\"}";
        }
        case GatewayMessageType::kToDeviceEvent: {
            auto payload_str = std::string(
                reinterpret_cast<const char*>(msg.m_payload.data()),
                msg.m_payload.size());
            return "{\"type\":\"event\",\"event\":\"" +
                   payload_str + "\",\"session_id\":\"" +
                   msg.m_session_id + "\"}";
        }
        case GatewayMessageType::kToDeviceText: {
            auto payload_str = std::string(
                reinterpret_cast<const char*>(msg.m_payload.data()),
                msg.m_payload.size());
            return "{\"type\":\"text\",\"content\":\"" +
                   payload_str + "\",\"session_id\":\"" +
                   msg.m_session_id + "\"}";
        }
        case GatewayMessageType::kToDeviceFunctionCall: {
            auto payload_str = std::string(
                reinterpret_cast<const char*>(msg.m_payload.data()),
                msg.m_payload.size());
            return "{\"type\":\"function_call\"," + payload_str +
                   ",\"session_id\":\"" + msg.m_session_id + "\"}";
        }
        case GatewayMessageType::kToDeviceConfigAck: {
            return "{\"type\":\"config_update_ack\",\"result\":\"ok\","
                   "\"session_id\":\"" + msg.m_session_id + "\"}";
        }
        case GatewayMessageType::kToDeviceConfigErr: {
            auto payload_str = std::string(
                reinterpret_cast<const char*>(msg.m_payload.data()),
                msg.m_payload.size());
            return "{\"type\":\"config_update_err\",\"code\":\"" +
                   payload_str + "\",\"session_id\":\"" +
                   msg.m_session_id + "\"}";
        }
        case GatewayMessageType::kToDeviceAudioChunk: {
            return std::string(
                reinterpret_cast<const char*>(msg.m_payload.data()),
                msg.m_payload.size());
        }
        case GatewayMessageType::kToDevicePong: {
            return "{\"type\":\"pong\",\"session_id\":\"" +
                   msg.m_session_id + "\"}";
        }
        default: {
            return std::string(
                reinterpret_cast<const char*>(msg.m_payload.data()),
                msg.m_payload.size());
        }
        }
    }

    void on_tight_message(const std::string& peer_id, tight::Bytes payload) {
        GatewayMessage msg;
        if (!parse_upstream_json(peer_id, payload, msg)) return;

        auto* entry = m_board->device(msg.m_device_id);
        {
            std::lock_guard<std::mutex> lock(entry->m_mutex);
            if (entry->m_data.m_peer_id.empty()) {
                entry->m_data.m_peer_id = peer_id;
            }
        }

        m_metrics->on_message_in(msg.m_device_id,
            std::to_string(static_cast<int>(msg.m_type)));
        m_metrics->on_bytes_in(payload.size());

        m_inbox->try_send(std::move(msg));
    }

    bool parse_upstream_json(const std::string& peer_id,
                             const Bytes& payload,
                             GatewayMessage& msg) {
        std::string json(reinterpret_cast<const char*>(payload.data()),
                         payload.size());
        msg.m_timestamp_ms = tight::unix_millis();
        msg.m_direction = MessageDirection::DeviceToGateway;

        if (json.find("\"hello\"") != std::string::npos ||
            json.find("\"product_id\"") != std::string::npos) {
            msg.m_type = GatewayMessageType::kDeviceHello;
            msg.m_device_id = extract_json_field(json, "device_name");
            if (msg.m_device_id.empty()) {
                msg.m_device_id = peer_id;
            }
            msg.m_payload = payload;
            return true;
        }
        if (json.find("\"config_update\"") != std::string::npos) {
            msg.m_type = GatewayMessageType::kDeviceConfigUpdate;
            msg.m_device_id = extract_json_field(json, "device_id");
            msg.m_payload = payload;
            return true;
        }
        if (json.find("\"function_call_output\"") != std::string::npos) {
            msg.m_type = GatewayMessageType::kDeviceFunctionCallOutput;
            msg.m_device_id = extract_json_field(json, "device_id");
            msg.m_payload = payload;
            return true;
        }
        if (json.find("\"audio_boundary\"") != std::string::npos) {
            msg.m_type = GatewayMessageType::kDeviceAudioBoundary;
            msg.m_device_id = extract_json_field(json, "device_id");
            msg.m_payload = payload;
            return true;
        }
        if (json.find("\"bye\"") != std::string::npos) {
            msg.m_type = GatewayMessageType::kDeviceBye;
            msg.m_device_id = extract_json_field(json, "device_id");
            msg.m_payload = payload;
            return true;
        }
        if (json.find("\"ping\"") != std::string::npos) {
            msg.m_type = GatewayMessageType::kDevicePing;
            msg.m_device_id = extract_json_field(json, "device_id");
            msg.m_payload = payload;
            return true;
        }

        msg.m_type = GatewayMessageType::kDeviceAudioFrame;
        msg.m_device_id = peer_id;
        msg.m_payload = payload;
        return true;
    }

    void on_tight_peer_event(const tight::PeerEvent& event) {
        switch (event.state) {
        case tight::LinkState::Online:
        case tight::LinkState::Established: {
            bool is_new = false;
            {
                auto* entry = m_board->device(event.id);
                std::lock_guard<std::mutex> lock(entry->m_mutex);
                if (!entry->m_data.m_connected) {
                    entry->m_data.m_connected = true;
                    entry->m_data.m_peer_id = event.id;
                    is_new = true;
                }
            }
            if (is_new) {
                m_session->create_session(event.id);
                TIGHT_LOG_INFO("Device online: " + event.id);

                GatewayMessage event_msg;
                event_msg.m_type = GatewayMessageType::kToDeviceEvent;
                event_msg.m_direction = MessageDirection::GatewayToDevice;
                event_msg.m_device_id = event.id;
                std::string ev = "connected";
                event_msg.m_payload = Bytes(ev.begin(), ev.end());
                send_to_device(event.id, event_msg);

                GatewayMessage status_msg;
                status_msg.m_type = GatewayMessageType::kToDeviceStatus;
                status_msg.m_direction = MessageDirection::GatewayToDevice;
                status_msg.m_device_id = event.id;
                std::string st = "idle";
                status_msg.m_payload = Bytes(st.begin(), st.end());
                send_to_device(event.id, status_msg);
            }
            break;
        }
        case tight::LinkState::Closed: {
            m_board->mark_changed("device:" + event.id);
            TIGHT_LOG_INFO("Device offline: " + event.id);
            break;
        }
        default: break;
        }
    }

    void process_message_batch(std::vector<GatewayMessage>& batch) {
        for (auto& msg : batch) {
            process_one_message(msg);
        }
        execute_pending_io();
    }

    // Dispatches one message to all listeners registered for its type
    // (observer pattern). A failing listener does not affect the others.
    void process_one_message(const GatewayMessage& msg) {
        std::shared_ptr<const ListenerList> listeners;
        {
            std::lock_guard<std::mutex> lock(m_listeners_mutex);
            auto it = m_listeners.find(static_cast<int>(msg.m_type));
            if (it != m_listeners.end()) listeners = it->second;
        }
        if (!listeners) return;
        for (const auto& listener : *listeners) {
            try { listener(msg); } catch (...) {}
        }
    }

    // Builds the listener context and wires the built-in listeners (living
    // in their own headers) into the dispatch. Extension point: support for
    // new message types is added by registering more listeners, not by
    // modifying the dispatch.
    void register_builtin_listeners() {
        m_ctx = std::make_shared<GatewayContext>();
        m_ctx->m_session = m_session;
        m_ctx->m_ratelimiter = m_ratelimiter;
        m_ctx->m_metrics = m_metrics;
        m_ctx->m_schedule_io = [this](std::function<void()> op) {
            m_pending_io.push_back(std::move(op));
        };
        m_ctx->m_send_to_device =
            [this](const std::string& dev, const GatewayMessage& msg) {
                return send_to_device(dev, msg);
            };
        m_ctx->m_downstream = [this](const GatewayMessage& msg) {
            if (m_downstream_cb) m_downstream_cb(msg);
        };

        m_hello_listener = std::make_unique<HelloListener>(m_ctx);
        m_audio_listener = std::make_unique<AudioListener>(m_ctx);
        m_device_listener = std::make_unique<DeviceListener>(m_ctx);
        m_forward_listener = std::make_unique<ForwardListener>(m_ctx);

        for (auto& reg : m_hello_listener->listeners()) {
            add_message_listener(reg.first, std::move(reg.second));
        }
        for (auto& reg : m_audio_listener->listeners()) {
            add_message_listener(reg.first, std::move(reg.second));
        }
        for (auto& reg : m_device_listener->listeners()) {
            add_message_listener(reg.first, std::move(reg.second));
        }
        for (auto& reg : m_forward_listener->listeners()) {
            add_message_listener(reg.first, std::move(reg.second));
        }
    }

    void execute_pending_io() {
        auto ops = std::move(m_pending_io);
        for (auto& op : ops) {
            try { op(); } catch (...) {}
        }
    }

    AuthResult do_authenticate(const std::string& product_id,
                                const std::string& product_key,
                                const std::string& product_secret,
                                const std::string& device_name) {
        AuthResult result;
        if (product_id.empty() || product_key.empty() || device_name.empty()) {
            result.m_ok = false;
            result.m_error_code = "AUTH_FAILED";
            result.m_error_message = "Missing credentials";
            return result;
        }
        result.m_ok = true;
        result.m_device_id = "dev-" + device_name;
        result.m_session_id = "";
        result.m_jwt = "";
        result.m_expires_at = static_cast<std::uint64_t>(
            std::time(nullptr) + 86400);
        return result;
    }

    void heartbeat_check() {
        auto now = tight::unix_millis();
        auto snapshot = m_board->device_snapshot();
        for (const auto& [id, state] : snapshot) {
            if (!state.m_connected) continue;
            auto gap = now - state.m_last_activity_ms;
            auto timeout_ms = static_cast<std::uint64_t>(
                m_config.m_server.heartbeat_seconds) * 2000;
            if (gap > timeout_ms) {
                TIGHT_LOG_WARN("Device heartbeat timeout: " + id);
                m_session->close_session(id);
                m_metrics->on_connection_close(id);
                m_board->mark_changed("device:" + id);
            }
        }
    }

    void session_cleanup() {
        auto now = tight::unix_millis();
        auto snapshot = m_board->device_snapshot();
        for (const auto& [id, state] : snapshot) {
            if (state.m_connected) continue;
            auto gap = now - state.m_last_activity_ms;
            if (gap > static_cast<std::uint64_t>(
                    m_config.m_server.session_timeout.count()) * 1000) {
                m_board->remove_device(id);
                TIGHT_LOG_DEBUG("Session cleaned up: " + id);
            }
        }
    }

    GatewayConfig                       m_config;
    std::shared_ptr<DataBoard>          m_board;
    std::shared_ptr<CopyChannel<GatewayMessage>> m_inbox;
    std::shared_ptr<SessionManager>     m_session;
    std::shared_ptr<AuthService>        m_auth;
    std::shared_ptr<RateLimiter>        m_ratelimiter;
    std::shared_ptr<Metrics>            m_metrics;
    std::shared_ptr<Reactor>            m_reactor;
    std::unique_ptr<tight::TightTransport> m_transport;

    DownstreamMsgCallback               m_downstream_cb;
    DownstreamErrCallback               m_downstream_err_cb;

    // Message listeners by type (copy-on-write snapshot for lock-free
    // dispatch reads).
    using ListenerList = std::vector<MessageListener>;
    std::unordered_map<int, std::shared_ptr<const ListenerList>> m_listeners;
    mutable std::mutex                m_listeners_mutex;

    // Listener context + built-in listener objects (owned so the registered
    // lambdas capturing them stay valid).
    std::shared_ptr<GatewayContext>   m_ctx;
    std::unique_ptr<HelloListener>    m_hello_listener;
    std::unique_ptr<AudioListener>    m_audio_listener;
    std::unique_ptr<DeviceListener>   m_device_listener;
    std::unique_ptr<ForwardListener>  m_forward_listener;

    std::atomic<bool>                   m_running{false};
    std::vector<std::function<void()>>  m_pending_io;
};

} // namespace gateway
