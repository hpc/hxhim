#include "transport/Messages/Delete.hpp"
#include "utils/memory.hpp"

Transport::Request::Delete::Delete()
    : Request(DELETE),
      Single(),
      subject(nullptr),
      subject_len(0),
      predicate(nullptr),
      predicate_len(0)
{}

Transport::Request::Delete::~Delete()
{
    if (clean) {
        dealloc(subject);
        dealloc(predicate);
    }
}

std::size_t Transport::Request::Delete::size() const {
    return Request::size() + sizeof(ds_offset) +
        subject_len + sizeof(subject_len) +
        predicate_len + sizeof(predicate_len);
}

Transport::Response::Delete::Delete()
    : Response(DELETE),
      Single(),
      status(HXHIM_ERROR)
{}

Transport::Response::Delete::~Delete()
{}

std::size_t Transport::Response::Delete::size() const {
    return Response::size() + sizeof(ds_offset) +
        sizeof(status);
}
