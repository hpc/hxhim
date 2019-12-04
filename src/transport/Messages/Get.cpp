#include "transport/Messages/Get.hpp"
#include "utils/memory.hpp"

Transport::Request::Get::Get()
    : Request(GET),
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
        dealloc(subject);
        dealloc(predicate);
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

Transport::Response::Get::Get()
    : Response(GET),
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
        dealloc(subject);
        dealloc(predicate);
        dealloc(object);
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
