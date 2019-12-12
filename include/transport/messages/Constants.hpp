#ifndef HXHIM_TRANSPORT_CONSTANTS_HPP
#define HXHIM_TRANSPORT_CONSTANTS_HPP

#include <cstdint>

namespace Transport {
    enum Type : uint8_t {
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
