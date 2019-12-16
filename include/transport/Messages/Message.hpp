#ifndef TRANSPORT_MESSAGE_BASE_HPP
#define TRANSPORT_MESSAGE_BASE_HPP

#include <cstdint>

#include "transport/constants.hpp"
#include "utils/memory.hpp"

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
        BPUT,
        BGET,
        BGET2,
        BGETOP,
        BDELETE,
        SYNC,
        BHISTOGRAM,
    };

    // String prepresentation of Types
    static const char *TypeStr[];

    Message(const Direction dir, const Type type, const std::size_t max_count = 0);
    virtual ~Message();

    virtual std::size_t size() const;

    virtual int alloc(const std::size_t max);
    virtual int cleanup();

    Direction direction;
    Type type;
    int src;                   // range server ID, not backend ID
    int dst;                   // range server ID, not backend ID
    std::size_t max_count;
    std::size_t count;
    int *ds_offsets;           // datastore id on the dst range server
};

}

#endif
