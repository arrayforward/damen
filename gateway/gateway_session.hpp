#pragma once

#include "gateway/gateway_types.hpp"
#include "gateway/gateway_blackboard.hpp"

#include <memory>
#include <string>
#include <unordered_map>

namespace gateway {

class SessionManager {
public:
    explicit SessionManager(std::shared_ptr<DataBoard> board)
        : m_board(std::move(board)) {}

    std::string create_session(const std::string& device_id) {
        auto* entry = m_board->device(device_id);
        std::lock_guard<std::mutex> lock(entry->m_mutex);
        auto& ds = entry->m_data;
        ds.m_device_id = device_id;
        ds.m_session_id = generate_session_id(device_id);
        ds.m_connected = true;
        ds.m_last_activity_ms = now_ms();
        m_board->mark_changed("device:" + device_id);
        return ds.m_session_id;
    }

    bool activate_session(const std::string& device_id, const std::string& session_id) {
        auto* entry = m_board->device(device_id);
        std::lock_guard<std::mutex> lock(entry->m_mutex);
        auto& ds = entry->m_data;
        if (ds.m_session_id != session_id) return false;
        ds.m_connected = true;
        ds.m_status = ConvaiStatus::kIdle;
        ds.m_last_activity_ms = now_ms();
        entry->mark_dirty();
        return true;
    }

    void close_session(const std::string& device_id) {
        auto* entry = m_board->device(device_id);
        {
            std::lock_guard<std::mutex> lock(entry->m_mutex);
            auto& ds = entry->m_data;
            ds.m_connected = false;
            ds.m_status = ConvaiStatus::kIdle;
            ds.m_last_activity_ms = now_ms();
        }
        m_board->mark_changed("device:" + device_id);
    }

    void update_status(const std::string& device_id, ConvaiStatus status) {
        auto* entry = m_board->device(device_id);
        std::lock_guard<std::mutex> lock(entry->m_mutex);
        auto& ds = entry->m_data;
        ds.m_status = status;
        ds.m_last_activity_ms = now_ms();
        entry->mark_dirty();
    }

    void record_bytes_in(const std::string& device_id, std::uint64_t bytes) {
        auto* entry = m_board->device(device_id);
        std::lock_guard<std::mutex> lock(entry->m_mutex);
        entry->m_data.m_bytes_in += bytes;
        entry->m_data.m_last_activity_ms = now_ms();
    }

    void record_bytes_out(const std::string& device_id, std::uint64_t bytes) {
        auto* entry = m_board->device(device_id);
        std::lock_guard<std::mutex> lock(entry->m_mutex);
        entry->m_data.m_bytes_out += bytes;
        entry->m_data.m_last_activity_ms = now_ms();
    }

    void record_message(const std::string& device_id) {
        auto* entry = m_board->device(device_id);
        std::lock_guard<std::mutex> lock(entry->m_mutex);
        entry->m_data.m_message_count++;
    }

    bool is_connected(const std::string& device_id) {
        auto* entry = m_board->device(device_id);
        std::lock_guard<std::mutex> lock(entry->m_mutex);
        return entry->m_data.m_connected;
    }

    std::optional<DataBoard::DeviceState> device_state(const std::string& device_id) {
        auto* entry = m_board->device(device_id);
        std::lock_guard<std::mutex> lock(entry->m_mutex);
        return entry->m_data;
    }

private:
    std::string generate_session_id(const std::string& device_id) {
        auto now = now_ms();
        auto ptr = reinterpret_cast<std::uintptr_t>(this);
        return device_id + "-" + std::to_string(now) + "-" + std::to_string(ptr);
    }

    std::uint64_t now_ms() {
        using namespace std::chrono;
        return static_cast<std::uint64_t>(
            duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count());
    }

    std::shared_ptr<DataBoard> m_board;
};

} // namespace gateway
