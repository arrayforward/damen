#pragma once

// Listener for pass-through messages that are simply forwarded downstream:
// config updates and function-call outputs.

#include "gateway/gateway_context.hpp"

#include "creek/logger.hpp"

#include <utility>
#include <vector>

namespace gateway {

class ForwardListener {
public:
    explicit ForwardListener(std::shared_ptr<GatewayContext> ctx)
        : m_ctx(std::move(ctx)) {}

    std::vector<ListenerRegistration> listeners() {
        return {
            {GatewayMessageType::kDeviceConfigUpdate,
             [this](const GatewayMessage& msg) { on_config_update(msg); }},
            {GatewayMessageType::kDeviceFunctionCallOutput,
             [this](const GatewayMessage& msg) { on_function_call_output(msg); }},
        };
    }

private:
    void on_config_update(const GatewayMessage& msg) {
        CREEK_LOG_DEBUG("Config update from: " + msg.m_device_id);
        forward(msg);
    }

    void on_function_call_output(const GatewayMessage& msg) {
        CREEK_LOG_DEBUG("Function call output from: " + msg.m_device_id);
        forward(msg);
    }

    void forward(const GatewayMessage& msg) {
        if (m_ctx->m_downstream) {
            m_ctx->m_schedule_io([ctx = m_ctx, msg = msg]() {
                ctx->m_downstream(msg);
            });
        }
    }

    std::shared_ptr<GatewayContext> m_ctx;
};

} // namespace gateway
