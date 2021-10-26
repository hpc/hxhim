#include "message/Request.hpp"

Message::Request::Request::Request(const enum hxhim_op_t type)
    : Message(Direction::REQUEST, type, 0)
{}

Message::Request::Request::~Request() {}

void Message::Request::Request::alloc(const std::size_t max) {
    Message::alloc(max);
}

std::size_t Message::Request::Request::add(const std::size_t ds, const bool increment_count) {
    return Message::add(ds, increment_count);
}

int Message::Request::Request::cleanup() {
    return Message::cleanup();
}
