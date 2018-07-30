#include "transport/Sync.hpp"

Transport::Request::Sync::Sync()
    : Single(),
      Request(Message::SYNC)
{}

std::size_t Transport::Request::Sync::size() const {
    return sizeof(db_offset) + Request::size();
}

Transport::Response::Sync::Sync()
    : Single(),
      Response(Message::SYNC)
{}

std::size_t Transport::Response::Sync::size() const {
    return sizeof(db_offset) + Response::size();
}
