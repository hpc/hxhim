#include "transport/Messages/Response.hpp"

Transport::Response::Response::Response(const enum hxhim_op_t type)
    : Message(Message::RESPONSE, type, 0),
      statuses(nullptr),
      next(nullptr)
{}

Transport::Response::Response::~Response()
{}

void Transport::Response::Response::alloc(const std::size_t max) {
    if (max) {
        Message::alloc(max);
        statuses = alloc_array<int>(max);
    }
}

std::size_t Transport::Response::Response::add(const int status, const std::size_t ds, const bool increment_count) {
    statuses[count] = status;
    return Message::add(sizeof(status) + ds, increment_count);
}

int Transport::Response::Response::steal(Transport::Response::Response *from, const std::size_t i) {
    if (Message::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    statuses[count] = from->statuses[i];

    return TRANSPORT_SUCCESS;
}

int Transport::Response::Response::cleanup() {
    dealloc_array(statuses, max_count);
    statuses = nullptr;

    return Message::cleanup();
}
