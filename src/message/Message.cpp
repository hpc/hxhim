#include "message/Message.hpp"

Message::Message::Message(const Direction dir, const enum hxhim_op_t op, const std::size_t max_count)
    : direction(dir),
      op(op),
      src(-1),
      dst(-1),
      max_count(max_count),
      count(0),
      serialized_size(sizeof(direction) + sizeof(op) +
                      sizeof(src) + sizeof(dst) +
                      sizeof(count)),
      timestamps()
{}

Message::Message::~Message() {
    cleanup();
}

std::size_t Message::Message::add(const std::size_t ds, const bool increment_count ) {
    if (increment_count) {
        count++;
    }
    return (serialized_size += ds);
}

std::size_t Message::Message::Message::size() const {
    return serialized_size;
}

std::size_t Message::Message::Message::filled() const {
    return count;
}

void Message::Message::alloc(const std::size_t max) {
    // final child should call cleanup before calling alloc

    if ((max_count = max)) {
        timestamps.reqs = alloc_array<::Stats::Send>(max_count);
        timestamps.transport = {};
    }

    count = 0;
}

int ::Message::Message::steal(::Message::Message *from, const std::size_t i) {
    // no space for new data
    if (count >= max_count) {
        return MESSAGE_ERROR;
    }

    // no source
    if (!from) {
        return MESSAGE_ERROR;
    }

    // bad source index
    if (i >= from->count) {
        return MESSAGE_ERROR;
    }

    timestamps.reqs[count] = std::move(from->timestamps.reqs[i]);

    // do not steal transport timestamps here
    // since it belongs to all of the individual timestamps
    // call steal_timestamps after all individual timestamps have been taken

    // increment count in calling function

    return MESSAGE_SUCCESS;
}

int Message::Message::steal_timestamps(Message *from, const bool steal_individuals) {
    if (!from) {
        return MESSAGE_ERROR;
    }

    timestamps.allocate = std::move(from->timestamps.allocate);

    if (steal_individuals) {
        dealloc_array(timestamps.reqs, max_count);
        timestamps.reqs = std::move(from->timestamps.reqs);
        from->timestamps.reqs = nullptr;
    }

    timestamps.transport = std::move(from->timestamps.transport);

    return MESSAGE_SUCCESS;
}

int Message::Message::cleanup() {
    dealloc_array(timestamps.reqs, max_count);
    timestamps.reqs = nullptr;

    count = 0;
    max_count = 0;

    return MESSAGE_SUCCESS;
}
