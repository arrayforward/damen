#pragma once

// Listener for device onboarding: handles kDeviceHello (connection rate
// limit + authentication) and completes the handshake on the auth result.

#include "gateway/gateway_context.hpp"

#include "creek/logger.hpp"

#include <string>
#include <utility>
#include <vector>

namespace gateway {

class HelloListener {
public:
    explicit HelloListener(std::shared_ptr<GatewayContext> ctx)
        : m_ctx(std::move(ctx)) {}

    std::vector<ListenerRegistration> listeners() {
        return {
            {GatewayMessageType::kDeviceHello,
             [this](const GatewayMessage& msg) { on_hello(msg); }},
        };
    }

private:
    void on_hello(const GatewayMessage& msg) {
        std::string json(reinterpret_cast<const char*>(msg.m_payload.data()),
                         msg.m_payload.size());
        std::string product_id = extract_json_field(json, "product_id");
        std::string product_key = extract_json_field(json, "product_key");
        std::string product_secret = extract_json_field(json, "product_secret");
        std::string device_name = extract_json_field(json, "device_name");

        CREEK_LOG_INFO("Device hello from: " + device_name);

        auto rl = m_ctx->m_ratelimiter->check(device_name, "connection");
        if (!rl.m_allowed) {
            m_ctx->m_metrics->on_ratelimit_hit("connection");
            GatewayMessage err;
            err.m_type = GatewayMessageType::kToDeviceHelloErr;
            err.m_direction = MessageDirection::GatewayToDevice;
            err.m_device_id = device_name;
            err.m_payload = Bytes(
                reinterpret_cast<const std::uint8_t*>(rl.m_error_code.data()),
                reinterpret_cast<const std::uint8_t*>(rl.m_error_code.data()) +
                    rl.m_error_code.size());
            m_ctx->m_schedule_io(
                [ctx = m_ctx, dev = device_name, err = std::move(err)]() {
                    ctx->m_send_to_device(dev, err);
                });
            return;
        }

        m_ctx->m_auth->authenticate(product_id, product_key, product_secret, device_name,
            [this, msg](const AuthResult& result) {
                on_auth_result(msg, result);
            });
    }

    void on_auth_result(const GatewayMessage& msg, const AuthResult& result) {
        std::string device_id = msg.m_device_id;

        if (!result.m_ok) {
            m_ctx->m_metrics->on_auth_failure();
            GatewayMessage err;
            err.m_type = GatewayMessageType::kToDeviceHelloErr;
            err.m_direction = MessageDirection::GatewayToDevice;
            err.m_device_id = device_id;
            err.m_payload = Bytes(
                reinterpret_cast<const std::uint8_t*>(result.m_error_code.data()),
                reinterpret_cast<const std::uint8_t*>(result.m_error_code.data()) +
                    result.m_error_code.size());
            m_ctx->m_schedule_io(
                [ctx = m_ctx, dev = device_id, err = std::move(err)]() {
                    ctx->m_send_to_device(dev, err);
                });
            return;
        }

        m_ctx->m_metrics->on_auth_success();
        m_ctx->m_metrics->on_connection_open(device_id);
        std::string session_id = m_ctx->m_session->create_session(device_id);

        GatewayMessage ack;
        ack.m_type = GatewayMessageType::kToDeviceHelloAck;
        ack.m_direction = MessageDirection::GatewayToDevice;
        ack.m_device_id = device_id;
        ack.m_session_id = session_id;
        m_ctx->m_schedule_io(
            [ctx = m_ctx, dev = device_id, ack = std::move(ack)]() {
                ctx->m_send_to_device(dev, ack);
            });

        if (m_ctx->m_downstream) {
            GatewayMessage ds_msg;
            ds_msg.m_type = GatewayMessageType::kDeviceHello;
            ds_msg.m_direction = MessageDirection::DownstreamOut;
            ds_msg.m_device_id = device_id;
            ds_msg.m_session_id = session_id;
            ds_msg.m_payload = msg.m_payload;
            m_ctx->m_schedule_io([ctx = m_ctx, ds_msg = std::move(ds_msg)]() {
                ctx->m_downstream(ds_msg);
            });
        }

        CREEK_LOG_INFO("Device authenticated: " + device_id +
                       " session: " + session_id);
    }

    std::shared_ptr<GatewayContext> m_ctx;
};

} // namespace gateway
