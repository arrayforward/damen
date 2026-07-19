#pragma once

// Public packet codec for the tight wire format. The implementation lives in
// tight/packet_codec.cpp; CRC32 is implemented in tight/crc32.cpp.

#include "creek/types.hpp"

#include <cstddef>
#include <cstdint>

namespace creek {

class PacketCodec {
public:
    static Bytes encode(const PacketHeader& header, const Bytes& payload);
    static bool decode(const Bytes& datagram, PacketHeader& header, Bytes& payload);
    static std::uint32_t crc32(const std::uint8_t* data, std::size_t size);
};

} // namespace creek
