#ifndef TRANSPORT_CONSTANTS_HPP
#define TRANSPORT_CONSTANTS_HPP

#include <cstdint>

namespace Transport {
    enum MessageType : uint8_t {
        BPUT,
        BGET,
        BGET2,
        BGETOP,
        BDELETE,
        BHISTOGRAM
    };

    enum Direction : uint8_t {
        REQUEST,
        RESPONSE
    };
}

#endif
