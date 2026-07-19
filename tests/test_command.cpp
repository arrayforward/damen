#include "test_framework.hpp"

#include "tight/peer.hpp"
#include "tight/command.hpp"

#include <chrono>
#include <thread>

using namespace creek;
using namespace creek::tight_detail;

static PacketHeader cmd_hdr(std::uint32_t seq) {
    PacketHeader h{};
    h.sequence = seq;
    h.tick = 1;
    return h;
}

TEST_CASE("command_in_order_delivery") {
    Peer peer;
    auto r1 = CommandChannel::handle(peer, cmd_hdr(1), Bytes{'a'}, 10000);
    ASSERT_EQ(r1.size(), 1u);
    auto r2 = CommandChannel::handle(peer, cmd_hdr(2), Bytes{'b'}, 10000);
    ASSERT_EQ(r2.size(), 1u);
    ASSERT_TRUE(peer.m_cmd_held.empty());
    ASSERT_EQ(peer.m_cmd_next_expected, 3u);
}

TEST_CASE("command_first_packet_initializes_sequence") {
    Peer peer;
    auto r = CommandChannel::handle(peer, cmd_hdr(5), Bytes{'x'}, 10000);
    ASSERT_EQ(r.size(), 1u);  // first packet defines the sequence base
    ASSERT_EQ(peer.m_cmd_next_expected, 6u);
}

TEST_CASE("command_out_of_order_held_then_drained") {
    Peer peer;
    CommandChannel::handle(peer, cmd_hdr(1), Bytes{'1'}, 10000);
    auto held = CommandChannel::handle(peer, cmd_hdr(3), Bytes{'3'}, 10000);
    ASSERT_TRUE(held.empty());  // held, waiting for seq 2
    ASSERT_EQ(peer.m_cmd_held.size(), 1u);

    auto r = CommandChannel::handle(peer, cmd_hdr(2), Bytes{'2'}, 10000);
    ASSERT_EQ(r.size(), 2u);  // seq 2 now, then held seq 3 drains in order
    ASSERT_TRUE(r[0] == Bytes{'2'});
    ASSERT_TRUE(r[1] == Bytes{'3'});
    ASSERT_TRUE(peer.m_cmd_held.empty());
    ASSERT_EQ(peer.m_cmd_next_expected, 4u);
}

TEST_CASE("command_gap_expires_after_3_rtt") {
    Peer peer;
    CommandChannel::handle(peer, cmd_hdr(1), Bytes{'1'}, 10000);
    CommandChannel::handle(peer, cmd_hdr(3), Bytes{'3'}, 10000);  // held

    // 3 RTT = 30ms with rtt=10ms: not elapsed yet.
    auto none = CommandChannel::flush_expired(peer, 10000);
    ASSERT_TRUE(none.empty());

    // With rtt_us = 1 the wait is 3us; after 1ms the gap has expired.
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    auto r = CommandChannel::flush_expired(peer, 1);
    ASSERT_EQ(r.size(), 1u);
    ASSERT_TRUE(r[0] == Bytes{'3'});
    ASSERT_EQ(peer.m_cmd_next_expected, 4u);

    // The skipped seq 2 arrives late -> dropped, not delivered.
    auto late = CommandChannel::handle(peer, cmd_hdr(2), Bytes{'2'}, 10000);
    ASSERT_TRUE(late.empty());
    ASSERT_EQ(peer.m_cmd_next_expected, 4u);
}

TEST_CASE("command_duplicate_dropped") {
    Peer peer;
    CommandChannel::handle(peer, cmd_hdr(1), Bytes{'1'}, 10000);
    auto dup = CommandChannel::handle(peer, cmd_hdr(1), Bytes{'1'}, 10000);
    ASSERT_TRUE(dup.empty());
}

TEST_CASE("command_reset_on_handshake") {
    Peer peer;
    CommandChannel::handle(peer, cmd_hdr(7), Bytes{'7'}, 10000);
    CommandChannel::handle(peer, cmd_hdr(9), Bytes{'9'}, 10000);  // held
    CommandChannel::reset(peer);
    ASSERT_FALSE(peer.m_cmd_initialized);
    ASSERT_TRUE(peer.m_cmd_held.empty());
    ASSERT_EQ(peer.m_cmd_seq_out, 1u);
    // After reset the next packet re-initializes the channel.
    auto r = CommandChannel::handle(peer, cmd_hdr(1), Bytes{'1'}, 10000);
    ASSERT_EQ(r.size(), 1u);
}
