#include "transport/Messages/Request.hpp"

Transport::Request::Request::Request(Message::Type type)
    : Message(Message::REQUEST, type, 0)
{}

Transport::Request::Request::~Request()
{}

std::size_t Transport::Request::Request::size() const {
    return Message::size();
}

int Transport::Request::Request::alloc(const std::size_t max) {
    Request::cleanup();
    return Message::alloc(max);
}

int Transport::Request::Request::cleanup() {
    return Message::cleanup();
}
