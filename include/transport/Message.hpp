#ifndef TRANSPORT_MESSAGE_BASE_HPP
#define TRANSPORT_MESSAGE_BASE_HPP

#include <cstddef>

namespace Transport {

struct Message {
    /**
     * Whether this packet is a request or a response
     */
    enum Direction {
        NONE,
        REQUEST,
        RESPONSE,
    };

    /**
     * Type
     * List of available message types
     */
    enum Type {
        INVALID,
        PUT,
        GET,
        DELETE,
        BPUT,
        BGET,
        BGETOP,
        BDELETE,
        SYNC,
        HISTOGRAM,
    };

    Message(const Direction dir, const Type type);
    virtual ~Message();

    virtual std::size_t size() const;

    Direction direction;
    Type type;
    int src;                   // range server ID, not backend ID
    int dst;                   // range server ID, not backend ID

    // This value indicates whether or not the values in arrays should be deallocated.
    // It should be set when a packet is being unpacked.
    // This value is not sent across the transport.
    bool clean;
};

}

#endif
