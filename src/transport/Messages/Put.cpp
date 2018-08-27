#include "transport/Messages/Put.hpp"

Transport::Request::Put::Put(FixedBufferPool * arrays, FixedBufferPool *buffers)
    : Request(Message::PUT, arrays, buffers),
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
        buffers->release(subject);
        buffers->release(predicate);
        buffers->release(object);
    }
}

std::size_t Transport::Request::Put::size() const {
    return Request::size() + sizeof(ds_offset) +
        subject_len + sizeof(subject_len) +
        predicate_len + sizeof(predicate_len) +
        sizeof(object_type) + object_len + sizeof(object_len);
}

Transport::Response::Put::Put(FixedBufferPool * arrays, FixedBufferPool *buffers)
    : Response(Message::PUT, arrays, buffers),
      Single(),
      status(HXHIM_ERROR)
{}

Transport::Response::Put::~Put()
{}

std::size_t Transport::Response::Put::size() const {
    return Response::size() + sizeof(ds_offset) +
        sizeof(status);
}
