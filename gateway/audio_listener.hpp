#pragma once

// Listener for the device media path: audio frames (bandwidth rate limit +
// byte accounting + downstream forwarding) and audio boundaries.

#include "gateway/gateway_context.hpp"

#include "creek/logger.hpp"

#include <utility>
#include <vector>

namespace gateway {

class AudioListener {
public:
    explicit AudioListener(std::shared_ptr<GatewayContext> ctx)
        : m_ctx(std::move(ctx)) {}

    std::vector<ListenerRegistration> listeners() {
        return {
            {GatewayMessageType::kDeviceAudioFrame,
             [this](const GatewayMessage& msg) { on_audio_frame(msg); }},
            {GatewayMessageType::kDeviceAudioBoundary,
             [this](const GatewayMessage& msg) { on_audio_boundary(msg); }},
        };
    }

private:
    void on_audio_frame(const GatewayMessage& msg) {
        auto rl = m_ctx->m_ratelimiter->check(msg.m_device_id, "bandwidth",
                                              msg.m_payload.size());
        if (!rl.m_allowed) {
            m_ctx->m_metrics->on_ratelimit_hit("bandwidth");
            return;
        }
        m_ctx->m_session->record_bytes_in(msg.m_device_id, msg.m_payload.size());
        if (m_ctx->m_downstream) {
            m_ctx->m_schedule_io([ctx = m_ctx, msg = msg]() {
                ctx->m_downstream(msg);
            });
        }
    }

    void on_audio_boundary(const GatewayMessage& msg) {
        CREEK_LOG_DEBUG("Audio boundary from: " + msg.m_device_id);
        if (m_ctx->m_downstream) {
            m_ctx->m_schedule_io([ctx = m_ctx, msg = msg]() {
                ctx->m_downstream(msg);
            });
        }
    }

    std::shared_ptr<GatewayContext> m_ctx;
};

} // namespace gateway
