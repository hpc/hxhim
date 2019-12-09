#include "transport/Messages/Message.hpp"

const char * Transport::Message::TypeStr[]  = {
    "INVALID",
    "PUT",
    "GET",
    "GET2",
    "DELETE",
    "BPUT",
    "BGET",
    "BGET2",
    "BGETOP",
    "BDELETE",
    "SYNC",
    "HISTOGRAM",
    "BHISTOGRAM",
};

Transport::Message::Message(const Message::Direction dir, const Message::Type type)
    : direction(dir),
      type(type),
      src(-1),
      dst(-1),
      clean(false)
{}

Transport::Message::~Message() {}

std::size_t Transport::Message::Message::size() const {
    return sizeof(direction) + sizeof(type) + sizeof(src) + sizeof(dst);
}

void Transport::Message::Message::server_side_cleanup(void *) {}
