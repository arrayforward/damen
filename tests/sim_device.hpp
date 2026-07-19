#pragma once

#include "tight/tight.hpp"
#include "tight/logger.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace gateway {

class SimDevice {
public:
    struct RecvEntry {
        std::string m_json;
        std::uint64_t m_ts_ms{};
    };

    SimDevice(const std::string& device_name, std::uint16_t port,
              std::uint16_t local_port = 0)
        : m_device_name(device_name)
        , m_port(port)
        , m_local_port(local_port == 0 ? static_cast<std::uint16_t>(port + 1)
                                       : local_port) {
    }

    ~SimDevice() { stop(); }

    bool start() {
        tight::TightConfig tc;
        tc.bind = tight::NetAddress("127.0.0.1", m_local_port);
        tc.id = m_device_name;
        tc.token = "gateway-shared-secret";
        tc.role = tight::LinkRole::Leaf;
        tc.mtu = 1400;
        tc.heartbeat = std::chrono::seconds(5);
        tc.dead_timeout = std::chrono::seconds(30);

        m_transport = std::make_unique<tight::TightTransport>(tc);
        m_transport->set_message_callback(
            [this](const std::string&, tight::Bytes payload) {
                std::string json(payload.begin(), payload.end());
                std::lock_guard<std::mutex> lock(m_mutex);
                m_received.push_back({std::move(json), tight::unix_millis()});
                m_cv.notify_all();
            });
        m_transport->set_peer_callback(
            [this](const tight::PeerEvent& ev) {
                if (ev.state == tight::LinkState::Online ||
                    ev.state == tight::LinkState::Established) {
                    m_connected.store(true);
                } else if (ev.state == tight::LinkState::Closed) {
                    m_connected.store(false);
                }
            });

        if (!m_transport->start()) return false;

        tight::RemotePeer server;
        server.id = "gateway-" + std::to_string(m_port);
        server.address = tight::NetAddress("127.0.0.1", m_port);
        return m_transport->connect(server);
    }

    void stop() {
        if (m_transport) {
            m_transport->stop();
            m_transport.reset();
        }
    }

    bool send_json(const std::string& json) {
        if (!m_transport) return false;
        tight::Bytes payload(json.begin(), json.end());
        return m_transport->send("gateway-" + std::to_string(m_port),
                                  std::move(payload));
    }

    bool send_hello(const std::string& product_id = "test-pid",
                     const std::string& product_key = "test-pkey",
                     const std::string& product_secret = "test-psec") {
        std::string json = "{"
            "\"type\":\"hello\","
            "\"product_id\":\"" + product_id + "\","
            "\"product_key\":\"" + product_key + "\","
            "\"product_secret\":\"" + product_secret + "\","
            "\"device_name\":\"" + m_device_name + "\""
            "}";
        return send_json(json);
    }

    bool send_ping() {
        std::string json = "{\"type\":\"ping\",\"device_id\":\"" +
                           m_device_name + "\"}";
        return send_json(json);
    }

    bool send_bye() {
        std::string json = "{\"type\":\"bye\",\"device_id\":\"" +
                           m_device_name + "\"}";
        return send_json(json);
    }

    bool send_config_update(const std::string& config_json) {
        std::string json = "{\"type\":\"config_update\","
                           "\"device_id\":\"" + m_device_name + "\","
                           "\"body\":" + config_json + "}";
        return send_json(json);
    }

    bool send_audio_frame(const std::vector<std::uint8_t>& g711_data) {
        std::string json = "{\"type\":\"audio_frame\","
                           "\"device_id\":\"" + m_device_name + "\""
                           "}";
        return send_json(json);
    }

    bool send_audio_boundary(const std::string& event_type) {
        std::string json = "{\"type\":\"audio_boundary\","
                           "\"device_id\":\"" + m_device_name + "\","
                           "\"event\":\"" + event_type + "\""
                           "}";
        return send_json(json);
    }

    bool send_function_call_output(const std::string& call_id,
                                    const std::string& output) {
        std::string json = "{\"type\":\"function_call_output\","
                           "\"device_id\":\"" + m_device_name + "\","
                           "\"call_id\":\"" + call_id + "\","
                           "\"output\":\"" + output + "\""
                           "}";
        return send_json(json);
    }

    std::vector<RecvEntry> drain_received() {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto copy = m_received;
        m_received.clear();
        return copy;
    }

    bool wait_for_json_containing(const std::string& substr,
                                   std::chrono::milliseconds timeout
                                   = std::chrono::seconds(5)) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        std::unique_lock<std::mutex> lock(m_mutex);
        while (true) {
            for (const auto& e : m_received) {
                if (e.m_json.find(substr) != std::string::npos)
                    return true;
            }
            if (m_cv.wait_until(lock, deadline) == std::cv_status::timeout)
                return false;
        }
    }

    bool wait_for_connected(std::chrono::milliseconds timeout
                            = std::chrono::seconds(5)) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (m_connected.load()) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return m_connected.load();
    }

    bool has_received_containing(const std::string& substr) {
        std::lock_guard<std::mutex> lock(m_mutex);
        for (const auto& e : m_received) {
            if (e.m_json.find(substr) != std::string::npos) return true;
        }
        return false;
    }

    std::size_t received_count() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_received.size();
    }

    void clear_received() {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_received.clear();
    }

    bool is_connected() const { return m_connected.load(); }

private:
    std::string                       m_device_name;
    std::uint16_t                     m_port;
    std::uint16_t                     m_local_port;
    std::unique_ptr<tight::TightTransport> m_transport;
    std::atomic<bool>                 m_connected{false};
    mutable std::mutex                m_mutex;
    std::vector<RecvEntry>            m_received;
    std::condition_variable           m_cv;
};

} // namespace gateway
