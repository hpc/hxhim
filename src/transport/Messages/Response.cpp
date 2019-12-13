#include "transport/Messages/Response.hpp"

Transport::Response::Response::Response(Message::Type type)
    : Message(Message::RESPONSE, type, 0)
{}

Transport::Response::Response::~Response()
{}

std::size_t Transport::Response::Response::size() const {
    return Message::size();
}
