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

int Transport::Response::Response::steal(Transport::Response::Response *res, const int ds) {
    if (!res) {
        return TRANSPORT_ERROR;
    }

    // cannot copy all
    if ((count + res->count) > max_count) {
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < res->count; i++) {
        ds_offsets[count] = ds;
        statuses[count] = res->statuses[i];

        res->statuses[i] = -1;
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Response::Response::cleanup() {
    dealloc_array(statuses, count);
    statuses = nullptr;

    return Message::cleanup();
}
