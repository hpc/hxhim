#include "transport/Response.hpp"

Transport::Response::Response::Response(Message::Type type)
    : Message(Message::RESPONSE, type)
{}

Transport::Response::Response::~Response()
{}

std::size_t Transport::Response::Response::size() const {
    return Message::size();
}
