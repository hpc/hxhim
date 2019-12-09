#include "transport/Messages/Request.hpp"

Transport::Request::Request::Request(Message::Type type)
    : Message(Message::REQUEST, type)
{}

Transport::Request::Request::~Request()
{}

std::size_t Transport::Request::Request::size() const {
    return Message::size();
}
