#pragma once

#include "gateway/gateway_types.hpp"

#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace gateway {

template <typename T>
struct BlackboardEntry {
    T              m_data;
    mutable std::mutex m_mutex;
    bool           m_dirty{false};

    void mark_dirty() { m_dirty = true; }
    void clear_dirty() { m_dirty = false; }
};

class DataBoard {
public:
    struct DeviceState {
        std::string      m_device_id;
        std::string      m_session_id;
        std::string      m_peer_id;
        std::string      m_jwt;
        bool             m_connected{false};
        bool             m_authenticated{false};
        ConvaiStatus     m_status{ConvaiStatus::kIdle};
        std::uint64_t    m_bytes_in{};
        std::uint64_t    m_bytes_out{};
        std::uint64_t    m_message_count{};
        std::uint64_t    m_last_activity_ms{};
        std::uint64_t    m_audio_duration_ms{};
    };

    struct DownstreamState {
        std::string      m_session_id;
        bool             m_stream_open{false};
        std::uint64_t    m_last_heartbeat_ms{};
    };

    struct MetricsState {
        std::uint64_t    m_active_connections{};
        std::uint64_t    m_messages_in_total{};
        std::uint64_t    m_messages_out_total{};
        std::uint64_t    m_ratelimit_hits_total{};
        std::uint64_t    m_auth_success_total{};
        std::uint64_t    m_auth_failure_total{};
        std::uint64_t    m_bytes_in_total{};
        std::uint64_t    m_bytes_out_total{};
    };

    DataBoard() = default;

    BlackboardEntry<DeviceState>* device(const std::string& device_id) {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto it = m_devices.find(device_id);
        if (it == m_devices.end()) {
            auto entry = std::make_unique<BlackboardEntry<DeviceState>>();
            entry->m_data.m_device_id = device_id;
            auto* ptr = entry.get();
            m_devices[device_id] = std::move(entry);
            return ptr;
        }
        return it->second.get();
    }

    BlackboardEntry<DownstreamState>* downstream(const std::string& session_id) {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto it = m_downstreams.find(session_id);
        if (it == m_downstreams.end()) {
            auto entry = std::make_unique<BlackboardEntry<DownstreamState>>();
            entry->m_data.m_session_id = session_id;
            auto* ptr = entry.get();
            m_downstreams[session_id] = std::move(entry);
            return ptr;
        }
        return it->second.get();
    }

    BlackboardEntry<MetricsState>* metrics() {
        return &m_metrics;
    }

    void remove_device(const std::string& device_id) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_devices.erase(device_id);
    }

    void remove_downstream(const std::string& session_id) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_downstreams.erase(session_id);
    }

    void mark_changed(const std::string& key) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_changed_keys.insert(key);
    }

    std::unordered_set<std::string> take_changed_keys() {
        std::lock_guard<std::mutex> lock(m_mutex);
        return std::move(m_changed_keys);
    }

    void register_evolver(const std::string& name,
                          std::function<void(DataBoard*)> fn) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_evolvers[name] = std::move(fn);
    }

    void evolve_once() {
        std::unordered_map<std::string, std::function<void(DataBoard*)>> evolvers_copy;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            evolvers_copy = m_evolvers;
        }
        auto keys = take_changed_keys();
        if (keys.empty()) return;
        for (const auto& key : keys) {
            (void)key;
            for (auto& [name, fn] : evolvers_copy) {
                fn(this);
            }
        }
    }

    std::unordered_map<std::string, DeviceState> device_snapshot() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::unordered_map<std::string, DeviceState> out;
        for (const auto& [id, entry] : m_devices) {
            std::lock_guard<std::mutex> elock(entry->m_mutex);
            out[id] = entry->m_data;
        }
        return out;
    }

    MetricsState metrics_snapshot() const {
        std::lock_guard<std::mutex> lock(m_metrics.m_mutex);
        return m_metrics.m_data;
    }

private:
    mutable std::mutex m_mutex;
    std::unordered_map<std::string, std::unique_ptr<BlackboardEntry<DeviceState>>> m_devices;
    std::unordered_map<std::string, std::unique_ptr<BlackboardEntry<DownstreamState>>> m_downstreams;
    BlackboardEntry<MetricsState> m_metrics;
    std::unordered_set<std::string> m_changed_keys;
    std::unordered_map<std::string, std::function<void(DataBoard*)>> m_evolvers;
};

} // namespace gateway
