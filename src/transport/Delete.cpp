#include "transport/Delete.hpp"

Transport::Request::Delete::Delete()
    : Request(Message::DELETE),
      Single(),
      subject(nullptr),
      subject_len(0),
      predicate(nullptr),
      predicate_len(0)
{}

Transport::Request::Delete::~Delete()
{
    if (clean) {
        ::operator delete(subject);
        ::operator delete(predicate);
    }
}

std::size_t Transport::Request::Delete::size() const {
    return Request::size() + sizeof(db_offset) +
        subject_len + sizeof(subject_len) +
        predicate_len + sizeof(predicate_len);
}

Transport::Response::Delete::Delete()
    : Response(Message::DELETE),
      Single(),
      status(TRANSPORT_ERROR)
{}

Transport::Response::Delete::~Delete()
{}

std::size_t Transport::Response::Delete::size() const {
    return Response::size() + sizeof(db_offset) +
        sizeof(status);
}
