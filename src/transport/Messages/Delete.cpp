#include "transport/Messages/Delete.hpp"

Transport::Request::Delete::Delete(FixedBufferPool *fbp)
    : Request(Message::DELETE, fbp),
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
    return Request::size() + sizeof(ds_offset) +
        subject_len + sizeof(subject_len) +
        predicate_len + sizeof(predicate_len);
}

Transport::Response::Delete::Delete(FixedBufferPool *fbp)
    : Response(Message::DELETE, fbp),
      Single(),
      status(HXHIM_ERROR)
{}

Transport::Response::Delete::~Delete()
{}

std::size_t Transport::Response::Delete::size() const {
    return Response::size() + sizeof(ds_offset) +
        sizeof(status);
}
