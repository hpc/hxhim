#ifndef TRANSPORT_MESSAGE_BASE_HPP
#define TRANSPORT_MESSAGE_BASE_HPP

#include <cstddef>

#include "utils/FixedBufferPool.hpp"

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
        GET2,
        DELETE,
        BPUT,
        BGET,
        BGET2,
        BGETOP,
        BDELETE,
        SYNC,
        HISTOGRAM,
        BHISTOGRAM,
    };

    // String prepresentation of Types
    static const char *TypeStr[];

    Message(const Direction dir, const Type type,
            FixedBufferPool *arrays, FixedBufferPool *buffers);
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

    FixedBufferPool *arrays;
    FixedBufferPool *buffers;
};

}

#endif
