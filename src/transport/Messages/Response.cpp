#include "transport/Messages/Response.hpp"

Transport::Response::Response::Response(Message::Type type, FixedBufferPool *fbp)
    : Message(Message::RESPONSE, type, fbp)
{}

Transport::Response::Response::~Response()
{}

std::size_t Transport::Response::Response::size() const {
    return Message::size();
}
