#include "transport/Messages/Get2.hpp"
#include "utils/memory.hpp"

Transport::Request::Get2::Get2()
    : Request(GET2),
      Single(),
      subject(nullptr),
      subject_len(0),
      predicate(nullptr),
      predicate_len(0),
      object_type(),
      object(nullptr),
      object_len(nullptr)
{}

Transport::Request::Get2::~Get2()
{
    subject = nullptr;
    predicate = nullptr;
    object = nullptr;
    object_len = nullptr;
}

std::size_t Transport::Request::Get2::size() const {
    return Request::size() + sizeof(ds_offset) +
        subject_len + sizeof(subject_len) +
        predicate_len + sizeof(predicate_len) +
        sizeof(object_type) + *object_len + sizeof(object_len);
}

Transport::Response::Get2::Get2()
    : Response(GET2),
      Single(),
      status(HXHIM_ERROR),
      subject(nullptr),
      subject_len(0),
      predicate(nullptr),
      predicate_len(0),
      object_type(),
      object(nullptr),
      object_len(nullptr)
{}

Transport::Response::Get2::~Get2()
{
    subject = nullptr;
    predicate = nullptr;
    object = nullptr;
}

std::size_t Transport::Response::Get2::size() const {
    return Response::size() + sizeof(ds_offset) +
        sizeof(status) +
        subject_len + sizeof(subject_len) +
        predicate_len + sizeof(predicate_len) +
        sizeof(object_type) + *object_len + sizeof(object_len);
}
