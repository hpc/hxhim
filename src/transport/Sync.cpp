#include "transport/Sync.hpp"

Transport::Request::Sync::Sync()
    : Request(Message::SYNC),
      Single()
{}

std::size_t Transport::Request::Sync::size() const {
    return sizeof(ds_offset) + Request::size();
}

Transport::Response::Sync::Sync()
    : Response(Message::SYNC),
      Single()
{}

std::size_t Transport::Response::Sync::size() const {
    return sizeof(ds_offset) + Response::size();
}
