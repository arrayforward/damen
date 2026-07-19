#pragma once

// Internal inbound data path: receive-sequence gap tracking, fragment
// collection, FEC recovery and message delivery. Not part of the public API.

#include "tight/types.hpp"

#include <cstdint>
#include <functional>

namespace tight::tight_detail {

struct Peer;
struct IncomingMessage;

class Reassembler {
public:
    using DeliverCallback = std::function<void(Peer* peer, Bytes payload)>;

    // Handles a Data/Parity packet: updates receive-sequence bookkeeping,
    // late-packet accounting (a packet is late when its one-way transit time,
    // derived from its send tick and the per-peer clock offset, exceeds
    // late_multiplier * rtt_us), collects fragments, and delivers each
    // completed message (recovered via FEC when needed) through the callback.
    static void handle_data(Peer& peer, const PacketHeader& header,
                            const Bytes& payload, std::uint32_t rtt_us,
                            double late_multiplier, const DeliverCallback& deliver);

private:
    static bool try_assemble(Peer& peer, IncomingMessage& in,
                             const DeliverCallback& deliver);
};

}
