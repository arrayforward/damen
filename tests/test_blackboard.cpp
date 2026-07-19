#include "tests/test_framework.hpp"
#include "gateway/gateway_blackboard.hpp"

#include <string>
#include <vector>

TEST_CASE("blackboard_device_create_and_read") {
    gateway::DataBoard board;
    auto* entry = board.device("dev-01");
    ASSERT_NE(entry, nullptr);
    {
        std::lock_guard<std::mutex> lock(entry->m_mutex);
        ASSERT_EQ(entry->m_data.m_device_id, "dev-01");
        ASSERT_FALSE(entry->m_data.m_connected);
    }
}

TEST_CASE("blackboard_device_same_instance_returned") {
    gateway::DataBoard board;
    auto* e1 = board.device("dev-01");
    auto* e2 = board.device("dev-01");
    ASSERT_EQ(e1, e2);
}

TEST_CASE("blackboard_device_modify_and_read") {
    gateway::DataBoard board;
    auto* entry = board.device("dev-01");
    {
        std::lock_guard<std::mutex> lock(entry->m_mutex);
        entry->m_data.m_connected = true;
        entry->m_data.m_session_id = "sess-123";
    }
    auto* e2 = board.device("dev-01");
    {
        std::lock_guard<std::mutex> lock(e2->m_mutex);
        ASSERT_TRUE(e2->m_data.m_connected);
        ASSERT_EQ(e2->m_data.m_session_id, "sess-123");
    }
}

TEST_CASE("blackboard_remove_device") {
    gateway::DataBoard board;
    board.device("dev-01");
    board.remove_device("dev-01");
    auto* entry = board.device("dev-01");
    {
        std::lock_guard<std::mutex> lock(entry->m_mutex);
        ASSERT_EQ(entry->m_data.m_device_id, "dev-01");
        ASSERT_FALSE(entry->m_data.m_connected);
    }
}

TEST_CASE("blackboard_downstream_create") {
    gateway::DataBoard board;
    auto* entry = board.downstream("sess-001");
    ASSERT_NE(entry, nullptr);
    {
        std::lock_guard<std::mutex> lock(entry->m_mutex);
        ASSERT_EQ(entry->m_data.m_session_id, "sess-001");
        ASSERT_FALSE(entry->m_data.m_stream_open);
    }
}

TEST_CASE("blackboard_downstream_remove") {
    gateway::DataBoard board;
    board.downstream("sess-001");
    board.remove_downstream("sess-001");
    auto* entry = board.downstream("sess-001");
    {
        std::lock_guard<std::mutex> lock(entry->m_mutex);
        ASSERT_FALSE(entry->m_data.m_stream_open);
    }
}

TEST_CASE("blackboard_mark_and_take_changed_keys") {
    gateway::DataBoard board;
    board.mark_changed("device:A");
    board.mark_changed("device:B");
    board.mark_changed("device:A");
    auto keys = board.take_changed_keys();
    ASSERT_EQ(keys.size(), 2u);
    ASSERT_TRUE(keys.count("device:A") > 0);
    ASSERT_TRUE(keys.count("device:B") > 0);
    keys = board.take_changed_keys();
    ASSERT_TRUE(keys.empty());
}

TEST_CASE("blackboard_device_snapshot") {
    gateway::DataBoard board;
    auto* e1 = board.device("dev-a");
    {
        std::lock_guard<std::mutex> lock(e1->m_mutex);
        e1->m_data.m_connected = true;
    }
    auto* e2 = board.device("dev-b");
    {
        std::lock_guard<std::mutex> lock(e2->m_mutex);
        e2->m_data.m_connected = false;
    }
    auto snap = board.device_snapshot();
    ASSERT_EQ(snap.size(), 2u);
    ASSERT_TRUE(snap["dev-a"].m_connected);
    ASSERT_FALSE(snap["dev-b"].m_connected);
}

TEST_CASE("blackboard_metrics_initial_zero") {
    gateway::DataBoard board;
    auto ms = board.metrics_snapshot();
    ASSERT_EQ(ms.m_active_connections, 0u);
    ASSERT_EQ(ms.m_messages_in_total, 0u);
    ASSERT_EQ(ms.m_ratelimit_hits_total, 0u);
}

TEST_CASE("blackboard_metrics_increment") {
    gateway::DataBoard board;
    {
        auto* entry = board.metrics();
        std::lock_guard<std::mutex> lock(entry->m_mutex);
        entry->m_data.m_active_connections = 42;
        entry->m_data.m_auth_success_total = 10;
    }
    auto ms = board.metrics_snapshot();
    ASSERT_EQ(ms.m_active_connections, 42u);
    ASSERT_EQ(ms.m_auth_success_total, 10u);
}

TEST_CASE("blackboard_evolve_once_single_layer") {
    gateway::DataBoard board;
    std::vector<std::string> calls;
    board.register_evolver("check_connect", [&calls](gateway::DataBoard* b) {
        calls.push_back("evolver_run");
        b->mark_changed("new:trigger");
    });
    board.mark_changed("device:A");
    board.evolve_once();
    ASSERT_EQ(calls.size(), 1u);
    ASSERT_EQ(calls[0], "evolver_run");
    auto remaining = board.take_changed_keys();
    ASSERT_TRUE(remaining.count("new:trigger") > 0);
}

TEST_CASE("blackboard_evolve_once_no_changes") {
    gateway::DataBoard board;
    int call_count = 0;
    board.register_evolver("noop", [&call_count](gateway::DataBoard*) {
        ++call_count;
    });
    board.evolve_once();
    ASSERT_EQ(call_count, 0);
}

TEST_CASE("blackboard_dirty_flag") {
    gateway::BlackboardEntry<int> entry;
    entry.m_data = 0;
    ASSERT_FALSE(entry.m_dirty);
    entry.mark_dirty();
    ASSERT_TRUE(entry.m_dirty);
    entry.clear_dirty();
    ASSERT_FALSE(entry.m_dirty);
}

TEST_CASE("blackboard_multiple_devices") {
    gateway::DataBoard board;
    for (int i = 0; i < 100; ++i) {
        auto* e = board.device("dev-" + std::to_string(i));
        std::lock_guard<std::mutex> lock(e->m_mutex);
        e->m_data.m_connected = (i % 2 == 0);
    }
    auto snap = board.device_snapshot();
    ASSERT_EQ(snap.size(), 100u);
    for (int i = 0; i < 100; ++i) {
        ASSERT_EQ(snap["dev-" + std::to_string(i)].m_connected, (i % 2 == 0));
    }
}
