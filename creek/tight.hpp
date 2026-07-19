#pragma once

#include "creek/types.hpp"
#include "creek/tight/packet_codec.hpp"
#include "creek/tight/fec.hpp"
#include "creek/tight/bandwidth.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace creek {

class TightTransport {
public:
    using MessageCallback = std::function<void(const std::string& peer_id, Bytes payload)>;
    using PeerCallback    = std::function<void(const PeerEvent& event)>;
    using CommandCallback = std::function<void(const std::string& peer_id, Bytes payload)>;

    explicit TightTransport(TightConfig config);
    ~TightTransport();

    TightTransport(const TightTransport&) = delete;
    TightTransport& operator=(const TightTransport&) = delete;

    void set_message_callback(MessageCallback callback);
    void set_peer_callback(PeerCallback callback);
    void set_command_callback(CommandCallback callback);

    bool start();
    void stop();

    bool connect(const RemotePeer& remote);
    bool send(const std::string& peer_id, Bytes payload);
    bool send_priority(const std::string& peer_id, Bytes payload, int priority);

    // Sends a command packet (control / button-key information). Commands
    // fit in a single datagram, so no fragmentation is needed, and they jump
    // the queue ahead of any pending data. The peer receives them via its
    // CommandCallback in order: out-of-order packets are held for at most
    // 3 RTT before the gap is skipped (late arrivals are then dropped).
    // Returns false when the payload exceeds one datagram.
    bool send_command(const std::string& peer_id, Bytes payload);

    std::vector<PeerEvent> peers() const;
    std::uint16_t local_port() const;

private:
    class Impl;
    std::unique_ptr<Impl> m_impl;
};

} // namespace creek
