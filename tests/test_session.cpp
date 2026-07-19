#include "tests/test_framework.hpp"
#include "gateway/gateway_session.hpp"

TEST_CASE("session_create_session") {
    auto board = std::make_shared<gateway::DataBoard>();
    gateway::SessionManager mgr(board);
    std::string sid = mgr.create_session("dev-01");
    ASSERT_FALSE(sid.empty());
    ASSERT_TRUE(sid.find("dev-01") != std::string::npos);
}

TEST_CASE("session_create_session_sets_connected") {
    auto board = std::make_shared<gateway::DataBoard>();
    gateway::SessionManager mgr(board);
    mgr.create_session("dev-01");
    ASSERT_TRUE(mgr.is_connected("dev-01"));
}

TEST_CASE("session_close_session") {
    auto board = std::make_shared<gateway::DataBoard>();
    gateway::SessionManager mgr(board);
    mgr.create_session("dev-01");
    ASSERT_TRUE(mgr.is_connected("dev-01"));
    mgr.close_session("dev-01");
    ASSERT_FALSE(mgr.is_connected("dev-01"));
}

TEST_CASE("session_activate_invalid_session") {
    auto board = std::make_shared<gateway::DataBoard>();
    gateway::SessionManager mgr(board);
    mgr.create_session("dev-01");
    bool ok = mgr.activate_session("dev-01", "wrong-session");
    ASSERT_FALSE(ok);
}

TEST_CASE("session_update_status") {
    auto board = std::make_shared<gateway::DataBoard>();
    gateway::SessionManager mgr(board);
    mgr.create_session("dev-01");
    mgr.update_status("dev-01", gateway::ConvaiStatus::kListening);
    auto state = mgr.device_state("dev-01");
    ASSERT_TRUE(state.has_value());
    ASSERT_EQ(static_cast<int>(state->m_status),
              static_cast<int>(gateway::ConvaiStatus::kListening));
}

TEST_CASE("session_status_transitions") {
    auto board = std::make_shared<gateway::DataBoard>();
    gateway::SessionManager mgr(board);
    mgr.create_session("dev-01");
    mgr.update_status("dev-01", gateway::ConvaiStatus::kListening);
    mgr.update_status("dev-01", gateway::ConvaiStatus::kThinking);
    mgr.update_status("dev-01", gateway::ConvaiStatus::kAnswering);
    auto state = mgr.device_state("dev-01");
    ASSERT_EQ(static_cast<int>(state->m_status),
              static_cast<int>(gateway::ConvaiStatus::kAnswering));
}

TEST_CASE("session_record_bytes_in") {
    auto board = std::make_shared<gateway::DataBoard>();
    gateway::SessionManager mgr(board);
    mgr.create_session("dev-01");
    mgr.record_bytes_in("dev-01", 1024);
    mgr.record_bytes_in("dev-01", 2048);
    auto state = mgr.device_state("dev-01");
    ASSERT_EQ(state->m_bytes_in, 3072u);
}

TEST_CASE("session_record_bytes_out") {
    auto board = std::make_shared<gateway::DataBoard>();
    gateway::SessionManager mgr(board);
    mgr.create_session("dev-01");
    mgr.record_bytes_out("dev-01", 512);
    mgr.record_bytes_out("dev-01", 256);
    auto state = mgr.device_state("dev-01");
    ASSERT_EQ(state->m_bytes_out, 768u);
}

TEST_CASE("session_record_message_count") {
    auto board = std::make_shared<gateway::DataBoard>();
    gateway::SessionManager mgr(board);
    mgr.create_session("dev-01");
    mgr.record_message("dev-01");
    mgr.record_message("dev-01");
    mgr.record_message("dev-01");
    auto state = mgr.device_state("dev-01");
    ASSERT_EQ(state->m_message_count, 3u);
}

TEST_CASE("session_multiple_devices") {
    auto board = std::make_shared<gateway::DataBoard>();
    gateway::SessionManager mgr(board);
    auto s1 = mgr.create_session("dev-A");
    auto s2 = mgr.create_session("dev-B");
    ASSERT_NE(s1, s2);
    ASSERT_TRUE(mgr.is_connected("dev-A"));
    ASSERT_TRUE(mgr.is_connected("dev-B"));
    mgr.close_session("dev-A");
    ASSERT_FALSE(mgr.is_connected("dev-A"));
    ASSERT_TRUE(mgr.is_connected("dev-B"));
}

TEST_CASE("session_close_session_mark_changed") {
    auto board = std::make_shared<gateway::DataBoard>();
    gateway::SessionManager mgr(board);
    mgr.create_session("dev-01");
    board->take_changed_keys();
    mgr.close_session("dev-01");
    auto keys = board->take_changed_keys();
    ASSERT_TRUE(keys.count("device:dev-01") > 0);
}
