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

    // any special cleanup to do on the server (database) side
    virtual void server_side_cleanup(void *args = nullptr);

    Direction direction;
    Type type;
    int src;                   // range server ID, not backend ID
    int dst;                   // range server ID, not backend ID
    int *ds_offsets;           // datastore id on the dst range server
    std::size_t count;
    std::size_t max_count;

    // This value indicates whether or not the values in arrays should be deallocated.
    // It should be set when a packet is being unpacked.
    // This value is not sent across the transport.
    bool clean;
};

}

#endif
