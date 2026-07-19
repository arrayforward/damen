#pragma once

// Shared context handed to the gateway message listeners: service handles
// plus IO hooks bound by the GatewayServer. Listeners never touch the
// server directly; everything flows through this context.

#include "gateway/gateway_types.hpp"
#include "gateway/gateway_session.hpp"
#include "gateway/gateway_auth.hpp"
#include "gateway/gateway_ratelimit.hpp"
#include "gateway/gateway_metrics.hpp"

#include <functional>
#include <memory>
#include <string>
#include <utility>

namespace gateway {

// Listener callback for a gateway message (observer pattern).
using MessageListener = std::function<void(const GatewayMessage& msg)>;
// One (message type, listener) registration entry.
using ListenerRegistration = std::pair<GatewayMessageType, MessageListener>;

struct GatewayContext {
    std::shared_ptr<SessionManager> m_session;
    std::shared_ptr<AuthService>    m_auth;        // bound in start()
    std::shared_ptr<RateLimiter>    m_ratelimiter;
    std::shared_ptr<Metrics>        m_metrics;

    // Deferred IO: listeners queue outbound work here; the reactor executes
    // it after each message batch.
    std::function<void(std::function<void()>)> m_schedule_io;
    // Sends a message to a device (GatewayServer::send_to_device).
    std::function<bool(const std::string& device_id,
                       const GatewayMessage& msg)> m_send_to_device;
    // Forwards a message downstream; empty when no downstream is set.
    std::function<void(const GatewayMessage& msg)> m_downstream;
};

} // namespace gateway
