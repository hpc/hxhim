#include "transport/Put.hpp"

Transport::Request::Put::Put()
    : Request(Message::PUT),
      Single(),
      subject(nullptr),
      subject_len(0),
      predicate(nullptr),
      predicate_len(0),
      object_type(),
      object(nullptr),
      object_len(0)
{}

Transport::Request::Put::~Put()
{
    if (clean) {
        ::operator delete(subject);
        ::operator delete(predicate);
        ::operator delete(object);
    }
}

std::size_t Transport::Request::Put::size() const {
    return Request::size() + sizeof(ds_offset) +
        subject_len + sizeof(subject_len) +
        predicate_len + sizeof(predicate_len) +
        sizeof(object_type) + object_len + sizeof(object_len);
}

Transport::Response::Put::Put()
    : Response(Message::PUT),
      Single(),
      status(TRANSPORT_ERROR)
{}

Transport::Response::Put::~Put()
{}

std::size_t Transport::Response::Put::size() const {
    return Response::size() + sizeof(ds_offset) +
        sizeof(status);
}
