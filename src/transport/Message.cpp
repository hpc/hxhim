#include "transport/Message.hpp"

Transport::Message::Message(const Message::Direction dir, const Message::Type type)
    : direction(dir),
      type(type),
      src(-1),
      dst(-1),
      clean(false)
{}

Transport::Message::~Message()
{}

std::size_t Transport::Message::Message::size() const {
    return sizeof(direction) + sizeof(type) + sizeof(src) + sizeof(dst);
}
