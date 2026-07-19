#include "tests/test_framework.hpp"
#include "gateway/gateway_auth.hpp"

TEST_CASE("auth_validator_success") {
    gateway::AuthService auth([](const std::string& pid, const std::string& pkey,
                                  const std::string& psec, const std::string& dname) {
        gateway::AuthResult r;
        r.m_ok = true;
        r.m_device_id = "dev-" + dname;
        r.m_jwt = "jwt-" + dname;
        r.m_expires_at = 9999999999ULL;
        return r;
    });

    bool called = false;
    gateway::AuthResult captured;
    auth.authenticate("pid", "pkey", "psec", "dname",
        [&](const gateway::AuthResult& r) {
            called = true;
            captured = r;
        });

    ASSERT_TRUE(called);
    ASSERT_TRUE(captured.m_ok);
    ASSERT_EQ(captured.m_device_id, "dev-dname");
    ASSERT_EQ(captured.m_jwt, "jwt-dname");
}

TEST_CASE("auth_validator_failure") {
    gateway::AuthService auth([](const std::string&, const std::string&,
                                  const std::string&, const std::string&) {
        gateway::AuthResult r;
        r.m_ok = false;
        r.m_error_code = "AUTH_FAILED";
        r.m_error_message = "bad credentials";
        return r;
    });

    bool called = false;
    auth.authenticate("pid", "pkey", "psec", "dname",
        [&](const gateway::AuthResult& r) {
            called = true;
            ASSERT_FALSE(r.m_ok);
            ASSERT_EQ(r.m_error_code, "AUTH_FAILED");
        });
    ASSERT_TRUE(called);
}

TEST_CASE("auth_no_validator_defaults_to_ok") {
    gateway::AuthService auth(nullptr);
    bool called = false;
    auth.authenticate("pid", "pkey", "psec", "dname",
        [&](const gateway::AuthResult& r) {
            called = true;
            ASSERT_TRUE(r.m_ok);
            ASSERT_EQ(r.m_device_id, "dev-dname");
        });
    ASSERT_TRUE(called);
}

TEST_CASE("auth_jwt_validate_default_ok") {
    gateway::AuthService auth(nullptr);
    ASSERT_TRUE(auth.validate_jwt("any-jwt-string"));
}

TEST_CASE("auth_jwt_validate_custom_validator") {
    gateway::AuthService auth(nullptr);
    auth.set_jwt_validator([](const std::string& jwt) {
        return jwt == "valid-token";
    });
    ASSERT_TRUE(auth.validate_jwt("valid-token"));
    ASSERT_FALSE(auth.validate_jwt("invalid-token"));
}

TEST_CASE("auth_passes_all_params_to_validator") {
    std::string captured_pid, captured_pkey, captured_psec, captured_dname;
    gateway::AuthService auth([&](const std::string& pid, const std::string& pkey,
                                    const std::string& psec, const std::string& dname) {
        captured_pid = pid;
        captured_pkey = pkey;
        captured_psec = psec;
        captured_dname = dname;
        gateway::AuthResult r;
        r.m_ok = true;
        return r;
    });
    auth.authenticate("my_pid", "my_key", "my_secret", "my_device", nullptr);
    ASSERT_EQ(captured_pid, "my_pid");
    ASSERT_EQ(captured_pkey, "my_key");
    ASSERT_EQ(captured_psec, "my_secret");
    ASSERT_EQ(captured_dname, "my_device");
}
