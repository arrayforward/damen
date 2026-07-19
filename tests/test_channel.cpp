#include "tests/test_framework.hpp"
#include "gateway/gateway_channel.hpp"

#include <string>
#include <thread>
#include <vector>

TEST_CASE("channel_send_recv_single") {
    gateway::CopyChannel<int> ch(10);
    ch.send(42);
    int val = 0;
    ASSERT_TRUE(ch.recv(val));
    ASSERT_EQ(val, 42);
}

TEST_CASE("channel_send_recv_multiple") {
    gateway::CopyChannel<int> ch(10);
    for (int i = 0; i < 5; ++i) ch.send(i * 10);
    for (int i = 0; i < 5; ++i) {
        int val = 0;
        ASSERT_TRUE(ch.recv(val));
        ASSERT_EQ(val, i * 10);
    }
}

TEST_CASE("channel_try_send_full") {
    gateway::CopyChannel<int> ch(2);
    ASSERT_TRUE(ch.try_send(1));
    ASSERT_TRUE(ch.try_send(2));
    ASSERT_FALSE(ch.try_send(3));

    int val = 0;
    ASSERT_TRUE(ch.recv(val));
    ASSERT_EQ(val, 1);
    ASSERT_TRUE(ch.try_send(3));
}

TEST_CASE("channel_try_recv_empty") {
    gateway::CopyChannel<int> ch(10);
    int val = 0;
    ASSERT_FALSE(ch.try_recv(val));
}

TEST_CASE("channel_try_recv_nonempty") {
    gateway::CopyChannel<int> ch(10);
    ch.send(77);
    int val = 0;
    ASSERT_TRUE(ch.try_recv(val));
    ASSERT_EQ(val, 77);
}

TEST_CASE("channel_size_tracking") {
    gateway::CopyChannel<int> ch(10);
    ASSERT_EQ(ch.size(), 0u);
    ch.send(1);
    ch.send(2);
    ch.send(3);
    ASSERT_EQ(ch.size(), 3u);
    int v = 0;
    ch.recv(v);
    ASSERT_EQ(ch.size(), 2u);
}

TEST_CASE("channel_close_blocks_send") {
    gateway::CopyChannel<int> ch(10);
    ch.close();
    ASSERT_FALSE(ch.try_send(1));
    ASSERT_TRUE(ch.is_closed());
}

TEST_CASE("channel_close_wakes_recv") {
    gateway::CopyChannel<int> ch(10);
    ch.close();
    int v = 0;
    ASSERT_FALSE(ch.recv(v));
}

TEST_CASE("channel_move_semantics") {
    gateway::CopyChannel<std::string> ch(10);
    ch.send("hello");
    std::string out;
    ASSERT_TRUE(ch.recv(out));
    ASSERT_EQ(out, "hello");
}

TEST_CASE("channel_swap_out_all") {
    gateway::CopyChannel<int> ch(10);
    for (int i = 0; i < 5; ++i) ch.send(i);
    std::vector<int> batch;
    ch.swap_out(batch);
    ASSERT_EQ(batch.size(), 5u);
    ASSERT_EQ(ch.size(), 0u);
    for (int i = 0; i < 5; ++i) ASSERT_EQ(batch[i], i);
}

TEST_CASE("channel_swap_out_empty") {
    gateway::CopyChannel<int> ch(10);
    std::vector<int> batch;
    batch.push_back(999);
    ch.swap_out(batch);
    ASSERT_TRUE(batch.empty());
}

TEST_CASE("channel_concurrent_send_recv") {
    gateway::CopyChannel<int> ch(100);
    std::atomic<int> sum{0};
    std::thread producer([&ch]() {
        for (int i = 0; i < 100; ++i) ch.send(i);
    });
    std::thread consumer([&ch, &sum]() {
        for (int i = 0; i < 100; ++i) {
            int v = 0;
            ch.recv(v);
            sum += v;
        }
    });
    producer.join();
    consumer.join();
    ASSERT_EQ(sum.load(), 4950);
}

TEST_CASE("channel_struct_copy_semantics") {
    struct Item { int a; std::string b; };
    gateway::CopyChannel<Item> ch(10);
    Item it{42, "test"};
    ch.send(it);
    it.a = 99;
    Item out;
    ASSERT_TRUE(ch.recv(out));
    ASSERT_EQ(out.a, 42);
    ASSERT_EQ(out.b, "test");
}
