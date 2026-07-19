#include "test_framework.hpp"

#include "tight/peer.hpp"
#include "tight/reassembler.hpp"
#include "tight/fragmenter.hpp"
#include "tight/report.hpp"

#include "creek/tight.hpp"

#include <chrono>

using namespace creek;
using namespace creek::tight_detail;

TEST_CASE("fec_parity_count_from_late_ratio_entropy") {
    // H(0) = 0 -> minimum 1 parity.
    ASSERT_EQ(Fragmenter::compute_parity_count_for(0.0, 10), 1);
    // H(0.01) = 0.0808 -> *1.2 = 0.097 -> 10 frags -> 1; 20 frags -> 2.
    ASSERT_EQ(Fragmenter::compute_parity_count_for(0.01, 10), 1);
    ASSERT_EQ(Fragmenter::compute_parity_count_for(0.01, 20), 2);
    // H(0.05) = 0.2864 -> *1.2 = 0.3437 -> 4 frags -> 2.
    ASSERT_EQ(Fragmenter::compute_parity_count_for(0.05, 4), 2);
    // H(0.1) = 0.469 -> *1.2 = 0.563 -> 10 frags -> 6 -> capped at 3.
    ASSERT_EQ(Fragmenter::compute_parity_count_for(0.1, 10), 3);
    // H(0.5) = 1.0 -> *1.2 = 1.2 -> 10 frags -> 12 -> capped at 3.
    ASSERT_EQ(Fragmenter::compute_parity_count_for(0.5, 10), 3);
}

TEST_CASE("fec_xor_recover_one") {
    std::vector<Bytes> frags = {Bytes{1, 2, 3, 4}, Bytes{5, 6, 7, 8}, Bytes{9, 10, 11, 12}};
    Bytes p = ReedSolomon::parity(frags, 4);
    std::vector<Bytes> broken = {Bytes{1, 2, 3, 4}, Bytes(4, 0), Bytes{9, 10, 11, 12}};
    ASSERT_TRUE(ReedSolomon::recover_one(broken, p, 1, 4));
    Bytes expected{5, 6, 7, 8};
    ASSERT_TRUE(broken[1] == expected);
}

TEST_CASE("bandwidth_probe_on_stable_rtt") {
    // RTT stable at the floor -> primary signal says probe (1.25x).
    BandwidthEstimator bw(1000);
    for (int i = 0; i < 12; ++i) {
        bw.on_ack(10000, std::chrono::microseconds(10000));  // 1 MB/s
    }
    ASSERT_EQ(bw.bytes_per_second(), 1250000ULL);
}

TEST_CASE("bandwidth_drain_on_rising_rtt") {
    BandwidthEstimator bw(1000);
    for (int i = 0; i < 12; ++i) {
        bw.on_ack(10000, std::chrono::microseconds(10000));
    }
    // RTT climbs 10000 -> ~22000us (queue building): primary signal drains.
    for (int i = 0; i < 12; ++i) {
        bw.on_ack(10000, std::chrono::microseconds(25000));  // 0.4 MB/s
    }
    ASSERT_EQ(bw.bytes_per_second(), 300000ULL);  // 400000 * 0.75
}

TEST_CASE("bandwidth_late_ratio_is_secondary") {
    // High late ratio forces drain even when RTT says probe.
    BandwidthEstimator bw(1000);
    for (int i = 0; i < 12; ++i) {
        bw.on_ack(10000, std::chrono::microseconds(10000));
    }
    ASSERT_EQ(bw.bytes_per_second(), 1250000ULL);  // probing
    bw.on_late_ratio(0.5);
    ASSERT_EQ(bw.bytes_per_second(), 750000ULL);   // vetoed down to drain

    // Mild late ratio only vetoes the probe, no drain.
    BandwidthEstimator bw2(1000);
    for (int i = 0; i < 12; ++i) {
        bw2.on_ack(10000, std::chrono::microseconds(10000));
    }
    bw2.on_late_ratio(0.05);
    ASSERT_EQ(bw2.bytes_per_second(), 1000000ULL);  // cruise
}

TEST_CASE("bandwidth_pure_rtt_samples_and_seed") {
    BandwidthEstimator bw(1000);
    bw.on_ack(0, std::chrono::microseconds(20000));
    bw.on_ack(0, std::chrono::microseconds(10000));
    ASSERT_EQ(bw.rtt().count(), 18750);  // EWMA (20000*7 + 10000) / 8

    // Speed-test measurement becomes the new baseline and floor.
    bw.seed_bandwidth(8000000);
    ASSERT_EQ(bw.bytes_per_second(), 8000000ULL);
    bw.seed_bandwidth(0);  // ignored
    ASSERT_EQ(bw.bytes_per_second(), 8000000ULL);
}

TEST_CASE("transit_time_uses_clock_offset") {
    Peer peer;
    ASSERT_EQ(transit_time_us(peer, 100, 200), -1);  // not synced
    peer.m_clock_synced = true;
    peer.m_clock_offset_us = 5000;                   // remote 5ms ahead
    // sent at tick=100ms, arrived at local 120ms: (120-100)*1000 + 5000
    ASSERT_EQ(transit_time_us(peer, 100, 120), 25000);
    ASSERT_EQ(transit_time_us(peer, 0, 120), -1);    // no tick
}

TEST_CASE("reassembler_late_accounting") {
    Peer peer;
    peer.m_clock_synced = true;
    peer.m_clock_offset_us = 0;  // same clock
    bool delivered = false;
    auto deliver = [&](Peer*, Bytes) { delivered = true; };

    // Packet stamped 100ms ago with rtt 10ms and 4x threshold -> late.
    PacketHeader late_hdr{};
    late_hdr.sequence = 1;
    late_hdr.fragment_count = 0;
    late_hdr.tick = static_cast<std::uint32_t>((unix_millis() - 100) & 0xFFFFFFFFULL);
    Reassembler::handle_data(peer, late_hdr, Bytes{}, 10000, 4.0, deliver);
    ASSERT_EQ(peer.m_transit_samples, 1u);
    ASSERT_EQ(peer.m_late_samples, 1u);
    ASSERT_FALSE(delivered);

    // Packet stamped now -> on time.
    PacketHeader ok_hdr{};
    ok_hdr.sequence = 2;
    ok_hdr.fragment_count = 0;
    ok_hdr.tick = static_cast<std::uint32_t>(unix_millis() & 0xFFFFFFFFULL);
    Reassembler::handle_data(peer, ok_hdr, Bytes{}, 10000, 4.0, deliver);
    ASSERT_EQ(peer.m_transit_samples, 2u);
    ASSERT_EQ(peer.m_late_samples, 1u);

    // Unsynced clock -> no accounting at all.
    Peer peer2;
    Reassembler::handle_data(peer2, ok_hdr, Bytes{}, 10000, 4.0, deliver);
    ASSERT_EQ(peer2.m_transit_samples, 0u);
}

TEST_CASE("report_round_trip_late_ratio_and_probe_bw") {
    Peer rx;  // receiver side: builds the report
    rx.m_seq_initialized = true;
    rx.m_next_expected_seq = 6;
    rx.m_transit_samples = 10;
    rx.m_late_samples = 1;
    rx.m_probe_bw_bps = 12345678;

    Bytes payload = Report::build_payload(rx);
    ASSERT_EQ(payload.size(), 16u);        // no lost seqs + trailing bw field
    ASSERT_EQ(rx.m_transit_samples, 0u);   // counters reset per interval
    ASSERT_EQ(rx.m_probe_bw_bps, 0u);      // attached exactly once

    Peer sender_side;  // sender side: parses the report
    auto noop = [](Peer*, const PacketHeader&, const Bytes&) {};
    std::uint64_t bw = Report::handle(sender_side, payload, noop);
    ASSERT_EQ(bw, 12345678ULL);
    ASSERT_TRUE(sender_side.m_peer_late_ratio > 0.09 &&
                sender_side.m_peer_late_ratio < 0.11);
}

TEST_CASE("probe_train_finalize") {
    auto now = std::chrono::steady_clock::now();

    Peer done;
    done.m_probe_first = now - std::chrono::milliseconds(30);
    done.m_probe_last = now - std::chrono::milliseconds(25);
    done.m_probe_count = 2;
    done.m_probe_bytes = 24000;
    finalize_probe_train(done, now);
    // span = 5ms, 24000 wire bytes -> 4.8e6 bytes/s
    ASSERT_EQ(done.m_probe_bw_bps, 4800000ULL);
    ASSERT_EQ(done.m_probe_count, 0u);

    // Gap not elapsed -> train still in flight, not finalized.
    Peer active;
    active.m_probe_first = now - std::chrono::milliseconds(3);
    active.m_probe_last = now - std::chrono::milliseconds(1);
    active.m_probe_count = 2;
    active.m_probe_bytes = 4800;
    finalize_probe_train(active, now);
    ASSERT_EQ(active.m_probe_bw_bps, 0u);
    ASSERT_EQ(active.m_probe_count, 2u);
}
