#include "transport/Messages/BPut.hpp"

Transport::Request::BPut::BPut(const std::size_t max)
    : Request(BPUT),
      subjects(nullptr),
      predicates(nullptr),
      object_types(nullptr),
      objects(nullptr)
{
    alloc(max);
}

Transport::Request::BPut::~BPut() {
    cleanup();
}

std::size_t Transport::Request::BPut::size() const {
    std::size_t total = Request::size();
    for(std::size_t i = 0; i < count; i++) {
        total += subjects[i]->len + sizeof(subjects[i]->len) +
                 predicates[i]->len + sizeof(predicates[i]->len) +
                 sizeof(object_types[i]) + objects[i]->len + sizeof(objects[i]->len);
    }
    return total;
}

int Transport::Request::BPut::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Request::alloc(max) != TRANSPORT_SUCCESS)       ||
            !(subjects = alloc_array<Blob *>(max))           ||
            !(predicates = alloc_array<Blob *>(max))         ||
            !(object_types = alloc_array<hxhim_type_t>(max)) ||
            !(objects = alloc_array<Blob *>(max)))            {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BPut::steal(Transport::Request::BPut *from, const std::size_t i) {
    if (Request::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    subjects[count]       = from->subjects[i];
    predicates[count]     = from->predicates[i];
    object_types[count]   = from->object_types[i];
    objects[count]        = from->objects[i];
    count++;

    from->subjects[i]     = nullptr;
    from->predicates[i]   = nullptr;
    from->objects[i]      = nullptr;

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BPut::cleanup() {
    dealloc_array(subjects, count);
    subjects = nullptr;

    dealloc_array(predicates, count);
    predicates = nullptr;

    dealloc_array(object_types, count);
    object_types = nullptr;

    dealloc_array(objects, count);
    objects = nullptr;

    return Request::cleanup();
}

Transport::Response::BPut::BPut(const std::size_t max)
    : Response(Message::BPUT),
      next(nullptr)
{
    alloc(max);
}

Transport::Response::BPut::~BPut() {
    cleanup();
}

std::size_t Transport::Response::BPut::size() const {
    return Response::size();
}

int Transport::Response::BPut::alloc(const std::size_t max) {
    cleanup();
    return Response::alloc(max);
}

int Transport::Response::BPut::steal(Transport::Response::BPut *bput, const std::size_t i) {
    if (Response::steal(bput, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    count++;

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BPut::cleanup() {
    return Response::cleanup();
}
