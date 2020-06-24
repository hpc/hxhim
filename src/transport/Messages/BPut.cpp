#include "transport/Messages/BPut.hpp"

Transport::Request::BPut::BPut(const std::size_t max)
    : Request(BPUT),
      subjects(nullptr),
      predicates(nullptr),
      object_types(nullptr),
      objects(nullptr),
      orig()
{
    alloc(max);
}

Transport::Request::BPut::~BPut() {
    cleanup();
}

std::size_t Transport::Request::BPut::size() const {
    std::size_t total = Request::size();
    for(std::size_t i = 0; i < count; i++) {
        total += subjects[i]->len + sizeof(subjects[i]->len) + sizeof(orig.subjects[i]) +
                 predicates[i]->len + sizeof(predicates[i]->len) + sizeof(orig.predicates[i]) +
                 sizeof(object_types[i]) +
                 objects[i]->len + sizeof(objects[i]->len);
    }
    return total;
}

int Transport::Request::BPut::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Request::alloc(max) != TRANSPORT_SUCCESS)              ||
            !(subjects            = alloc_array<Blob *>(max))       ||
            !(predicates          = alloc_array<Blob *>(max))       ||
            !(object_types        = alloc_array<hxhim_type_t>(max)) ||
            !(objects             = alloc_array<Blob *>(max))       ||
            !(orig.subjects       = alloc_array<void *>(max))       ||
            !(orig.predicates     = alloc_array<void *>(max)))       {
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

    subjects[count]          = from->subjects[i];
    predicates[count]        = from->predicates[i];
    object_types[count]      = from->object_types[i];
    objects[count]           = from->objects[i];

    orig.subjects[count]     = from->orig.subjects[i];
    orig.predicates[count]   = from->orig.predicates[i];

    count++;

    from->subjects[i]        = nullptr;
    from->predicates[i]      = nullptr;
    from->objects[i]         = nullptr;

    from->orig.subjects[i]   = nullptr;
    from->orig.predicates[i] = nullptr;

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

    dealloc_array(orig.subjects, count);
    orig.subjects = nullptr;

    dealloc_array(orig.predicates, count);
    orig.predicates = nullptr;

    return Request::cleanup();
}

Transport::Response::BPut::BPut(const std::size_t max)
    : Response(Message::BPUT),
      orig(),
      next(nullptr)
{
    alloc(max);
}

Transport::Response::BPut::~BPut() {
    cleanup();
}

std::size_t Transport::Response::BPut::size() const {
    std::size_t total = Response::size();
    for(std::size_t i = 0; i < count; i++) {
        total += sizeof(orig.subjects[i]->ptr) + sizeof(orig.subjects[i]->len) +
                 sizeof(orig.predicates[i]->ptr) + sizeof(orig.predicates[i]->len);
    }
    return total;
}

int Transport::Response::BPut::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Response::alloc(max) != TRANSPORT_SUCCESS)                 ||
            !(orig.subjects        = alloc_array<ReferenceBlob *>(max)) ||
            !(orig.predicates      = alloc_array<ReferenceBlob *>(max))) {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BPut::steal(Transport::Response::BPut *from, const std::size_t i) {
    if (Response::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    orig.subjects[count]      = from->orig.subjects[i];
    orig.predicates[count]    = from->orig.predicates[i];

    count++;

    from->orig.subjects[i]    = nullptr;
    from->orig.predicates[i]  = nullptr;

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BPut::cleanup() {
    for(std::size_t i = 0; i < count; i++) {
        destruct(orig.subjects[i]);
        destruct(orig.predicates[i]);
    }

    dealloc_array(orig.subjects, count);
    orig.subjects = nullptr;

    dealloc_array(orig.predicates, count);
    orig.predicates = nullptr;

    return Response::cleanup();
}
