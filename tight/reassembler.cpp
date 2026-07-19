#include "reassembler.hpp"

#include "peer.hpp"
#include "wire_format.hpp"

#include "creek/tight/fec.hpp"
#include "creek/logger.hpp"

#include <algorithm>
#include <cstring>
#include <vector>

namespace creek::tight_detail {

void Reassembler::handle_data(Peer& peer, const PacketHeader& header,
                              const Bytes& payload, std::uint32_t rtt_us,
                              double late_multiplier, const DeliverCallback& deliver) {
    std::uint32_t seq = header.sequence;
    auto now = std::chrono::steady_clock::now();

    // One-way transit via the per-peer clock offset (computed outside the
    // lock: the clock fields are written exclusively on this receiver
    // thread). -1 means the clock is not synced yet -> no accounting.
    std::int64_t transit_us = transit_time_us(peer, header.tick, unix_millis());
    {
        // m_missing_seqs / m_recv_seqs / m_next_expected_seq /
        // m_transit_samples / m_late_samples are also mutated by the report
        // builder on the reactor thread. Guard them with peer.m_mu or the
        // two threads corrupt the std::map (rb_tree) internals -> crash in
        // _S_rebalance_for_erase.
        std::lock_guard<std::mutex> seq_lock(peer.m_mu);

        // Late-packet accounting: transit time above late_multiplier * RTT.
        if (transit_us >= 0) {
            ++peer.m_transit_samples;
            std::uint64_t threshold_us = static_cast<std::uint64_t>(
                late_multiplier * static_cast<double>(rtt_us > 0 ? rtt_us : 10000));
            if (static_cast<std::uint64_t>(transit_us) > threshold_us) {
                ++peer.m_late_samples;
            }
        }

        if (!peer.m_seq_initialized) {
            peer.m_next_expected_seq = seq + 1;
            peer.m_seq_initialized = true;
        } else if (seq >= peer.m_next_expected_seq) {
            for (std::uint32_t g = peer.m_next_expected_seq; g < seq; ++g) {
                if (!peer.m_recv_seqs.count(g)) {
                    peer.m_missing_seqs.emplace(g, now);
                }
            }
            peer.m_recv_seqs.insert(seq);
            while (peer.m_recv_seqs.count(peer.m_next_expected_seq)) {
                peer.m_recv_seqs.erase(peer.m_next_expected_seq);
                ++peer.m_next_expected_seq;
            }
        } else {
            auto mit = peer.m_missing_seqs.find(seq);
            if (mit != peer.m_missing_seqs.end()) {
                peer.m_missing_seqs.erase(mit);
            }
            while (peer.m_recv_seqs.count(peer.m_next_expected_seq)) {
                peer.m_recv_seqs.erase(peer.m_next_expected_seq);
                ++peer.m_next_expected_seq;
            }
        }
    }

    if (header.fragment_count == 0) return;
    {
        std::lock_guard<std::mutex> lock(peer.m_mu);
        if (peer.m_completed.find(header.message_id) != peer.m_completed.end()) return;
    }
    std::uint16_t idx = header.fragment_index;
    std::uint16_t cnt = header.fragment_count;
    if (idx >= cnt) return;
    auto& in = peer.m_incoming[header.message_id];
    {
        std::lock_guard<std::mutex> lock(peer.m_mu);
        if (in.m_message_id == 0) {
            in.m_message_id = header.message_id;
            in.m_total_count = cnt;
            in.m_data_count = header.flags;
            in.m_fragments.assign(cnt, std::nullopt);
            in.m_sizes.assign(cnt, 0);
            in.m_first_seen = now;
        }
        if (in.m_total_count != cnt) return;
        if (idx >= in.m_fragments.size()) return;
        if (!in.m_fragments[idx].has_value()) {
            in.m_fragments[idx] = payload;
            in.m_sizes[idx] = header.reserved;
        }
    }
    bool assembled = try_assemble(peer, in, deliver);
    if (assembled) {
        std::lock_guard<std::mutex> lock(peer.m_mu);
        peer.m_completed[header.message_id] = now;
        peer.m_incoming.erase(header.message_id);
    }
}

bool Reassembler::try_assemble(Peer& peer, IncomingMessage& in,
                               const DeliverCallback& deliver) {
    if (in.m_total_count < 2) return false;
    std::size_t data_count = in.m_data_count > 0 ? in.m_data_count : (in.m_total_count - 1);
    std::size_t parity_count = in.m_total_count - data_count;
    std::size_t have = 0;
    for (std::size_t i = 0; i < data_count; ++i) {
        if (in.m_fragments[i].has_value()) ++have;
    }
    auto build_msg = [&](const std::vector<Bytes>& parts, const std::vector<std::uint16_t>& real_sizes) -> Bytes {
        Bytes full;
        for (std::size_t i = 0; i < parts.size() && i < real_sizes.size(); ++i) {
            std::size_t n = std::min<std::size_t>(real_sizes[i], parts[i].size());
            full.insert(full.end(), parts[i].begin(), parts[i].begin() + n);
        }
        if (full.size() < 4) return Bytes{};
        std::uint32_t total_be = 0;
        std::memcpy(&total_be, full.data(), 4);
        std::uint32_t total = to_be32(total_be);
        if (total > full.size() - 4) total = static_cast<std::uint32_t>(full.size() - 4);
        return Bytes(full.begin() + 4, full.begin() + 4 + total);
    };
    if (have == data_count) {
        std::vector<Bytes> parts;
        std::vector<std::uint16_t> real_sizes;
        parts.reserve(data_count);
        real_sizes.reserve(data_count);
        for (std::size_t i = 0; i < data_count; ++i) {
            if (!in.m_fragments[i].has_value()) {
                CREEK_LOG_DEBUG(std::string("[tight] try_assemble missing frag i=") + std::to_string(i) +
                                 " data_count=" + std::to_string(data_count) +
                                 " total=" + std::to_string(in.m_total_count));
                return false;  // race; retry next fragment
            }
            parts.push_back(*in.m_fragments[i]);
            real_sizes.push_back(in.m_sizes[i]);
        }
        deliver(&peer, build_msg(parts, real_sizes));
        return true;
    }
    // 统计缺失的数据分片数
    std::size_t multi = 0;
    for (std::size_t i = 0; i < data_count; ++i) {
        if (!in.m_fragments[i].has_value()) ++multi;
    }
    if (multi > parity_count) return false;   // 缺失超过校验能力
    if (multi == 0) return false;
    std::size_t width = 0;
    for (auto& f : in.m_fragments) {
        if (f.has_value()) width = std::max(width, f->size());
    }
    if (width == 0) return false;

    // Reed-Solomon 解码：任意 multi 个缺失分片用任意 multi 个校验分片恢复
    std::vector<std::optional<Bytes>> data_frags(data_count);
    for (std::size_t i = 0; i < data_count; ++i) data_frags[i] = in.m_fragments[i];
    std::vector<std::pair<std::size_t, Bytes>> parities;
    for (std::size_t p = 0; p < parity_count; ++p) {
        if (data_count + p < in.m_fragments.size() &&
            in.m_fragments[data_count + p].has_value()) {
            parities.emplace_back(p, *in.m_fragments[data_count + p]);
        }
    }
    if (multi > parities.size()) return false;
    if (!ReedSolomon::decode(data_frags, parities, width)) return false;

    // 回填恢复出的分片；真实长度由报文内 4 字节总长前缀在 build_msg 裁剪
    for (std::size_t i = 0; i < data_count; ++i) {
        if (!in.m_fragments[i].has_value() && data_frags[i].has_value()) {
            in.m_fragments[i] = *data_frags[i];
            in.m_sizes[i] = static_cast<std::uint16_t>(width);
        }
    }
    std::vector<Bytes> recovered_parts(data_count);
    for (std::size_t i = 0; i < data_count; ++i) {
        recovered_parts[i] = *in.m_fragments[i];
    }
    deliver(&peer, build_msg(recovered_parts, in.m_sizes));
    return true;
}

}
