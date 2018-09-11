#include "transport/Messages/Message.hpp"

Transport::Message::Message(const Message::Direction dir, const Message::Type type, FixedBufferPool * arrays, FixedBufferPool *buffers)
    : direction(dir),
      type(type),
      src(-1),
      dst(-1),
      clean(false),
      arrays(arrays),
      buffers(buffers)
{}

Transport::Message::~Message() {}

std::size_t Transport::Message::Message::size() const {
    return sizeof(direction) + sizeof(type) + sizeof(src) + sizeof(dst);
}