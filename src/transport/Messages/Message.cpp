#include "transport/Messages/Message.hpp"

const char * Transport::Message::TypeStr[]  = {
    "INVALID",
    "BPUT",
    "BGET",
    "BGETOP",
    "BDELETE",
    "SYNC",
    "BHISTOGRAM",
};

Transport::Message::Message(const Message::Direction dir, const Message::Type type, const std::size_t max_count)
    : direction(dir),
      type(type),
      src(-1),
      dst(-1),
      max_count(max_count),
      count(0),
      ds_offsets(nullptr)
{
    Message::alloc(max_count);
}

Transport::Message::~Message() {
    cleanup();
}

std::size_t Transport::Message::Message::size() const {
    return sizeof(direction) + sizeof(type) + sizeof(src) + sizeof(dst) + (count * sizeof(*ds_offsets)) + sizeof(count);
}

int Transport::Message::alloc(const std::size_t max) {
    Message::cleanup();

    if ((max_count = max)) {
        if (!(ds_offsets = alloc_array<int>(max))) {
            cleanup();
            return TRANSPORT_ERROR;
        }

        count = 0;
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Message::cleanup() {
    dealloc_array(ds_offsets, count);
    ds_offsets = nullptr;

    count = 0;

    return TRANSPORT_SUCCESS;
}
