#include "transport/Messages/Get.hpp"

Transport::Request::Get::Get(FixedBufferPool * arrays, FixedBufferPool *buffers)
    : Request(Message::GET, arrays, buffers),
      Single(),
      subject(nullptr),
      subject_len(0),
      predicate(nullptr),
      predicate_len(0),
      object_type()
{}

Transport::Request::Get::~Get()
{
    if (clean) {
        buffers->release(subject);
        buffers->release(predicate);
    }

    subject = nullptr;
    predicate = nullptr;
}

std::size_t Transport::Request::Get::size() const {
    return Request::size() + sizeof(ds_offset) +
        subject_len + sizeof(subject_len) +
        predicate_len + sizeof(predicate_len) +
        sizeof(object_type);
}

Transport::Response::Get::Get(FixedBufferPool * arrays, FixedBufferPool *buffers)
    : Response(Message::GET, arrays, buffers),
      Single(),
      status(HXHIM_ERROR),
      subject(nullptr),
      subject_len(0),
      predicate(nullptr),
      predicate_len(0),
      object_type(),
      object(nullptr),
      object_len(0)
{}

Transport::Response::Get::~Get()
{
    if (clean) {
        buffers->release(subject);
        buffers->release(predicate);
        buffers->release(object);
    }

    subject = nullptr;
    predicate = nullptr;
    object = nullptr;
}

std::size_t Transport::Response::Get::size() const {
    return Response::size() + sizeof(ds_offset) +
        sizeof(status) +
        subject_len + sizeof(subject_len) +
        predicate_len + sizeof(predicate_len) +
        sizeof(object_type) + object_len + sizeof(object_len);
}
