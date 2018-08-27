#include "transport/Messages/Request.hpp"

Transport::Request::Request::Request(Message::Type type, FixedBufferPool * arrays, FixedBufferPool * buffers)
    : Message(Message::REQUEST, type, arrays, buffers)
{}

Transport::Request::Request::~Request()
{}

std::size_t Transport::Request::Request::size() const {
    return Message::size();
}
