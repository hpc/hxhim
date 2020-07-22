#include "transport/Messages/Message.hpp"

Transport::Message::Message(const Message::Direction dir, const enum hxhim_op_t op, const std::size_t max_count)
    : direction(dir),
      op(op),
      src(-1),
      dst(-1),
      max_count(max_count),
      count(0),
      ds_offsets(nullptr),
      timestamps()
{
    Message::alloc(max_count);
}

Transport::Message::~Message() {
    cleanup();
}

std::size_t Transport::Message::Message::size() const {
    return sizeof(direction) + sizeof(op) + sizeof(src) + sizeof(dst) + (count * sizeof(*ds_offsets)) + sizeof(count);
}

std::size_t Transport::Message::Message::filled() const {
    return count;
}

int Transport::Message::alloc(const std::size_t max) {
    Message::cleanup();

    if ((max_count = max)) {
        if (!(ds_offsets      = alloc_array<int>(max))                       ||
            !(timestamps.reqs = alloc_array<struct hxhim::Stats::Send>(max))) {
            cleanup();
            return TRANSPORT_ERROR;
        }

        count = 0;
    }

    return TRANSPORT_SUCCESS;
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

    ds_offsets[count] = from->ds_offsets[i];

    // timestamps.reqs[count] = from->timestamps.reqs[i];

    // increment count in calling function

    return TRANSPORT_SUCCESS;
}

int Transport::Message::cleanup() {
    dealloc_array(ds_offsets, max_count);
    ds_offsets = nullptr;

    dealloc_array(timestamps.reqs, max_count);
    timestamps.reqs = nullptr;

    count = 0;

    return TRANSPORT_SUCCESS;
}
