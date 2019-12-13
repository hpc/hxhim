#include "transport/Messages/Message.hpp"

const char * Transport::Message::TypeStr[]  = {
    "INVALID",
    "BPUT",
    "BGET",
    "BGET2",
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
      ds_offsets(nullptr),
      count(0),
      max_count(max_count),
      clean(false)
{
    Message::alloc(max_count);
}

Transport::Message::~Message() {}

std::size_t Transport::Message::Message::size() const {
    return sizeof(direction) + sizeof(type) + sizeof(src) + sizeof(dst);
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

void Transport::Message::Message::server_side_cleanup(void *) {}
