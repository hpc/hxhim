#include "transport/Messages/Message.hpp"

Transport::Message::Message(const Message::Direction dir, const enum hxhim_op_t op, const std::size_t max_count)
    : direction(dir),
      op(op),
      src(-1),
      dst(-1),
      max_count(max_count),
      count(0),
      timestamps()
{}

Transport::Message::~Message() {}

std::size_t Transport::Message::Message::size() const {
    static const std::size_t HEADER_SIZE = sizeof(direction) + sizeof(op) + sizeof(src) + sizeof(dst) + sizeof(count);
    return HEADER_SIZE;
}

std::size_t Transport::Message::Message::filled() const {
    return count;
}

void Transport::Message::alloc(const std::size_t max) {
    // final child should call cleanup before calling alloc

    if ((max_count = max)) {
        timestamps.reqs = alloc_array<::Stats::Send>(max_count);
        timestamps.transport = {};
    }

    count = 0;
}

int Transport::Message::steal(Transport::Message *from, const std::size_t i) {
    // no space for new data
    if (count >= max_count) {
        return TRANSPORT_ERROR;
    }

    // no source
    if (!from) {
        return TRANSPORT_ERROR;
    }

    // bad source index
    if (i >= from->count) {
        return TRANSPORT_ERROR;
    }

    timestamps.reqs[count] = std::move(from->timestamps.reqs[i]);

    // do not steal transport timestamps here
    // since it belongs to all of the individual timestamps
    // call steal_timestamps after all individual timestamps have been taken

    // increment count in calling function

    return TRANSPORT_SUCCESS;
}

int Transport::Message::steal_timestamps(Message *from, const bool steal_individuals) {
    if (!from) {
        return TRANSPORT_ERROR;
    }

    timestamps.allocate = std::move(from->timestamps.allocate);

    if (steal_individuals) {
        dealloc_array(timestamps.reqs, max_count);
        timestamps.reqs = std::move(from->timestamps.reqs);
        from->timestamps.reqs = nullptr;
    }

    timestamps.transport = std::move(from->timestamps.transport);

    return TRANSPORT_SUCCESS;
}

int Transport::Message::cleanup() {
    dealloc_array(timestamps.reqs, max_count);
    timestamps.reqs = nullptr;

    count = 0;
    max_count = 0;

    return TRANSPORT_SUCCESS;
}
