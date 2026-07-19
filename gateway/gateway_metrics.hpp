#pragma once

#include "gateway/gateway_blackboard.hpp"
#include "tight/logger.hpp"

#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>

namespace gateway {

class Metrics {
public:
    explicit Metrics(std::shared_ptr<DataBoard> board)
        : m_board(std::move(board)) {}

    void on_connection_open(const std::string& device_id) {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto* entry = m_board->metrics();
        std::lock_guard<std::mutex> mlock(entry->m_mutex);
        ++entry->m_data.m_active_connections;
    }

    void on_connection_close(const std::string& device_id) {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto* entry = m_board->metrics();
        std::lock_guard<std::mutex> mlock(entry->m_mutex);
        if (entry->m_data.m_active_connections > 0)
            --entry->m_data.m_active_connections;
    }

    void on_message_in(const std::string& device_id, const std::string& type) {
        auto* entry = m_board->metrics();
        std::lock_guard<std::mutex> mlock(entry->m_mutex);
        ++entry->m_data.m_messages_in_total;
    }

    void on_message_out(const std::string& device_id, const std::string& type) {
        auto* entry = m_board->metrics();
        std::lock_guard<std::mutex> mlock(entry->m_mutex);
        ++entry->m_data.m_messages_out_total;
    }

    void on_bytes_in(std::uint64_t bytes) {
        auto* entry = m_board->metrics();
        std::lock_guard<std::mutex> mlock(entry->m_mutex);
        entry->m_data.m_bytes_in_total += bytes;
    }

    void on_bytes_out(std::uint64_t bytes) {
        auto* entry = m_board->metrics();
        std::lock_guard<std::mutex> mlock(entry->m_mutex);
        entry->m_data.m_bytes_out_total += bytes;
    }

    void on_ratelimit_hit(const std::string& dimension) {
        auto* entry = m_board->metrics();
        std::lock_guard<std::mutex> mlock(entry->m_mutex);
        ++entry->m_data.m_ratelimit_hits_total;
    }

    void on_auth_success() {
        auto* entry = m_board->metrics();
        std::lock_guard<std::mutex> mlock(entry->m_mutex);
        ++entry->m_data.m_auth_success_total;
    }

    void on_auth_failure() {
        auto* entry = m_board->metrics();
        std::lock_guard<std::mutex> mlock(entry->m_mutex);
        ++entry->m_data.m_auth_failure_total;
    }

    std::string prometheus_text() const {
        auto snapshot = m_board->metrics_snapshot();
        std::ostringstream oss;
        oss << "# HELP gateway_active_connections Current device connections\n";
        oss << "# TYPE gateway_active_connections gauge\n";
        oss << "gateway_active_connections " << snapshot.m_active_connections << "\n";

        oss << "# HELP gateway_messages_in_total Total inbound messages\n";
        oss << "# TYPE gateway_messages_in_total counter\n";
        oss << "gateway_messages_in_total " << snapshot.m_messages_in_total << "\n";

        oss << "# HELP gateway_messages_out_total Total outbound messages\n";
        oss << "# TYPE gateway_messages_out_total counter\n";
        oss << "gateway_messages_out_total " << snapshot.m_messages_out_total << "\n";

        oss << "# HELP gateway_ratelimit_hits_total Rate limit hits\n";
        oss << "# TYPE gateway_ratelimit_hits_total counter\n";
        oss << "gateway_ratelimit_hits_total " << snapshot.m_ratelimit_hits_total << "\n";

        oss << "# HELP gateway_auth_success_total Successful authentications\n";
        oss << "# TYPE gateway_auth_success_total counter\n";
        oss << "gateway_auth_success_total " << snapshot.m_auth_success_total << "\n";

        oss << "# HELP gateway_auth_failure_total Failed authentications\n";
        oss << "# TYPE gateway_auth_failure_total counter\n";
        oss << "gateway_auth_failure_total " << snapshot.m_auth_failure_total << "\n";

        oss << "# HELP gateway_bytes_in_total Total bytes received\n";
        oss << "# TYPE gateway_bytes_in_total counter\n";
        oss << "gateway_bytes_in_total " << snapshot.m_bytes_in_total << "\n";

        oss << "# HELP gateway_bytes_out_total Total bytes sent\n";
        oss << "# TYPE gateway_bytes_out_total counter\n";
        oss << "gateway_bytes_out_total " << snapshot.m_bytes_out_total << "\n";

        return oss.str();
    }

    void log_snapshot() {
        auto snapshot = m_board->metrics_snapshot();
        TIGHT_LOG_INFO("Metrics| conn:" + std::to_string(snapshot.m_active_connections) +
                       " in:" + std::to_string(snapshot.m_messages_in_total) +
                       " out:" + std::to_string(snapshot.m_messages_out_total) +
                       " bytes_in:" + std::to_string(snapshot.m_bytes_in_total) +
                       " bytes_out:" + std::to_string(snapshot.m_bytes_out_total) +
                       " ratelimit:" + std::to_string(snapshot.m_ratelimit_hits_total));
    }

private:
    std::shared_ptr<DataBoard> m_board;
    std::mutex m_mutex;
};

} // namespace gateway
