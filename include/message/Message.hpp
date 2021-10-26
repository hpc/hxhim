#ifndef BASE_MESSAGE_HPP
#define BASE_MESSAGE_HPP

#include <cstdint>

#include "hxhim/constants.h"
#include "message/constants.hpp"
#include "utils/Stats.hpp"
#include "utils/memory.hpp"

namespace Message {
/**
 * Whether this packet is a request or a response
 */
enum Direction {
    NONE,
    REQUEST,
    RESPONSE,
};

struct Message {
    Message(const Direction dir, const enum hxhim_op_t op, const std::size_t max_count = 0);
    virtual ~Message();

    virtual std::size_t add(const size_t ds, const bool increment_count);
    std::size_t size() const;
    std::size_t filled() const;

    virtual void alloc(const std::size_t max);
    virtual int steal(Message *from, const std::size_t i);
    int steal_timestamps(Message *from, const bool steal_individuals);
    virtual int cleanup();

    Direction direction;
    enum hxhim_op_t op;
    int src;
    int dst;
    std::size_t max_count;
    std::size_t count;
    std::size_t serialized_size;

    struct {
        Stats::Chronostamp allocate;
        struct Stats::Send *reqs;
        struct Stats::SendRecv transport;
    } timestamps;
};

}

#endif
