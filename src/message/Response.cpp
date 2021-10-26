#include "message/Response.hpp"

Message::Response::Response::Response(const enum hxhim_op_t type)
    : Message(Direction::RESPONSE, type, 0),
      statuses(nullptr),
      next(nullptr)
{}

Message::Response::Response::~Response()
{}

void Message::Response::Response::alloc(const std::size_t max) {
    if (max) {
        Message::alloc(max);
        statuses = alloc_array<int>(max);
    }
}

std::size_t Message::Response::Response::add(const int status, const std::size_t ds, const bool increment_count) {
    statuses[count] = status;
    return Message::add(sizeof(status) + ds, increment_count);
}

int Message::Response::Response::steal(Response *from, const std::size_t i) {
    if (Message::steal(from, i) != MESSAGE_SUCCESS) {
        return MESSAGE_ERROR;
    }

    statuses[count] = from->statuses[i];

    return MESSAGE_SUCCESS;
}

int Message::Response::Response::cleanup() {
    dealloc_array(statuses, max_count);
    statuses = nullptr;

    return Message::cleanup();
}
