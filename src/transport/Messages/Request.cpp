#include "transport/Messages/Request.hpp"

Transport::Request::Request::Request(Message::Type type, FixedBufferPool *fbp)
    : Message(Message::REQUEST, type, fbp)
{}

Transport::Request::Request::~Request()
{}

std::size_t Transport::Request::Request::size() const {
    return Message::size();
}
