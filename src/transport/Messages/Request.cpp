#include "transport/Messages/Request.hpp"

Transport::Request::Request::Request(const enum hxhim_op_t type)
    : Message(Message::REQUEST, type, 0)
{}

Transport::Request::Request::~Request() {
    cleanup();
}

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

int Transport::Request::Request::steal(Transport::Request::Request *from, const std::size_t i) {
    return Message::steal(from, i);
}
