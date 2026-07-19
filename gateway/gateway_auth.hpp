#pragma once

#include "gateway/gateway_types.hpp"

#include <functional>
#include <string>

namespace gateway {

class AuthService {
public:
    using AuthCallback = std::function<void(const AuthResult& result)>;

    explicit AuthService(std::function<AuthResult(const std::string&,
                                                    const std::string&,
                                                    const std::string&,
                                                    const std::string&)> validator)
        : m_validator(std::move(validator)) {}

    void authenticate(const std::string& product_id,
                      const std::string& product_key,
                      const std::string& product_secret,
                      const std::string& device_name,
                      AuthCallback callback) {
        AuthResult result;
        if (m_validator) {
            result = m_validator(product_id, product_key, product_secret, device_name);
        } else {
            result.m_ok = true;
            result.m_device_id = "dev-" + device_name;
            result.m_session_id = "sess-" + std::to_string(std::time(nullptr));
            result.m_jwt = "mock-jwt-" + device_name;
            result.m_expires_at = static_cast<std::uint64_t>(
                std::time(nullptr) + 86400);
        }
        if (callback) callback(result);
    }

    bool validate_jwt(const std::string& jwt) {
        if (m_jwt_validator) return m_jwt_validator(jwt);
        return true;
    }

    void set_jwt_validator(std::function<bool(const std::string&)> validator) {
        m_jwt_validator = std::move(validator);
    }

private:
    std::function<AuthResult(const std::string&, const std::string&,
                               const std::string&, const std::string&)> m_validator;
    std::function<bool(const std::string&)> m_jwt_validator;
};

} // namespace gateway
