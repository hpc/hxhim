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

int Transport::Request::BPut::cleanup() {
    for(std::size_t i = 0; i < count; i++) {
        destruct(subjects[i]);
        destruct(predicates[i]);
        destruct(objects[i]);
    }

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

int Transport::Response::BPut::cleanup() {
    return Response::cleanup();
}
