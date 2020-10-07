#ifndef TRANSPORT_MESSAGE_BASE_HPP
#define TRANSPORT_MESSAGE_BASE_HPP

#include <cstdint>
#include <memory>

#include "hxhim/constants.h"
#include "transport/constants.hpp"
#include "utils/Stats.hpp"
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

    Message(const Direction dir, const enum hxhim_op_t op, const std::size_t max_count = 0);
    virtual ~Message();

    virtual std::size_t size() const;
    std::size_t filled() const;

    virtual void alloc(const std::size_t max);
    void alloc_timestamps();
    virtual int steal(Message *from, const std::size_t i);
    int steal_timestamps(Message *from, const bool steal_individuals);
    virtual int cleanup();

    Direction direction;
    enum hxhim_op_t op;
    int src;                   // range server ID, not backend ID
    int dst;                   // range server ID, not backend ID
    std::size_t max_count;
    std::size_t count;

    struct {
        struct Stats::Send **reqs;
        std::shared_ptr<struct Stats::SendRecv> transport;
    } timestamps;
};

}

#endif
