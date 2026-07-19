#pragma once

// Listener for device lifecycle and housekeeping messages: bye (session
// teardown), ping (pong reply) and the internal rate-limit notification.

#include "gateway/gateway_context.hpp"

#include "tight/logger.hpp"

#include <utility>
#include <vector>

namespace gateway {

class DeviceListener {
public:
    explicit DeviceListener(std::shared_ptr<GatewayContext> ctx)
        : m_ctx(std::move(ctx)) {}

    std::vector<ListenerRegistration> listeners() {
        return {
            {GatewayMessageType::kDeviceBye,
             [this](const GatewayMessage& msg) { on_bye(msg); }},
            {GatewayMessageType::kDevicePing,
             [this](const GatewayMessage& msg) { on_ping(msg); }},
            {GatewayMessageType::kInternalRateLimit,
             [this](const GatewayMessage& msg) { on_rate_limit(msg); }},
        };
    }

private:
    void on_bye(const GatewayMessage& msg) {
        TIGHT_LOG_INFO("Device bye: " + msg.m_device_id);
        m_ctx->m_session->close_session(msg.m_device_id);
        m_ctx->m_metrics->on_connection_close(msg.m_device_id);
        if (m_ctx->m_downstream) {
            m_ctx->m_schedule_io([ctx = m_ctx, msg = msg]() {
                ctx->m_downstream(msg);
            });
        }
    }

    void on_ping(const GatewayMessage& msg) {
        GatewayMessage pong;
        pong.m_type = GatewayMessageType::kToDevicePong;
        pong.m_direction = MessageDirection::GatewayToDevice;
        pong.m_device_id = msg.m_device_id;
        pong.m_session_id = msg.m_session_id;
        m_ctx->m_schedule_io(
            [ctx = m_ctx, dev = msg.m_device_id, pong = std::move(pong)]() {
                ctx->m_send_to_device(dev, pong);
            });
    }

    void on_rate_limit(const GatewayMessage& msg) {
        m_ctx->m_metrics->on_ratelimit_hit("message");
    }

    std::shared_ptr<GatewayContext> m_ctx;
};

} // namespace gateway
