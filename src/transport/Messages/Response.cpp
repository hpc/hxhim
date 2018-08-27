#include "transport/Messages/Response.hpp"

Transport::Response::Response::Response(Message::Type type, FixedBufferPool *arrays, FixedBufferPool *buffers)
    : Message(Message::RESPONSE, type, arrays, buffers)
{}

Transport::Response::Response::~Response()
{}

std::size_t Transport::Response::Response::size() const {
    return Message::size();
}
