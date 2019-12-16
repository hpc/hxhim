#include "transport/Messages/Response.hpp"

Transport::Response::Response::Response(Message::Type type)
    : Message(Message::RESPONSE, type, 0),
      statuses(nullptr)
{}

Transport::Response::Response::~Response()
{}

std::size_t Transport::Response::Response::size() const {
    return Message::size() + (count * sizeof(*statuses));
}

int Transport::Response::Response::alloc(const std::size_t max) {
    Response::cleanup();

    if (max) {
        if ((Message::alloc(max) != TRANSPORT_SUCCESS) ||
            !(statuses = alloc_array<int>(max)))        {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Response::Response::cleanup() {
    dealloc_array(statuses, count);
    statuses = nullptr;

    return Message::cleanup();
}
