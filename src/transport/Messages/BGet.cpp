#include "transport/Messages/BGet.hpp"
#include <cstdio>

Transport::Request::BGet::BGet(const std::size_t max)
    : Request(hxhim_op_t::HXHIM_GET),
      subjects(nullptr),
      predicates(nullptr),
      object_types(nullptr),
      orig()
{
    alloc(max);
}

Transport::Request::BGet::~BGet() {
    cleanup();
}

std::size_t Transport::Request::BGet::size() const {
    std::size_t total = Request::size();
    for(std::size_t i = 0; i < count; i++) {
        total += subjects[i].pack_size() + sizeof(subjects[i].data()) +
                 predicates[i].pack_size() + sizeof(predicates[i].data()) +
                 sizeof(object_types[i]);
    }
    return total;
}

int Transport::Request::BGet::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Request::alloc(max) != TRANSPORT_SUCCESS)                     ||
            !(subjects            = alloc_array<Blob>(max))                ||
            !(predicates          = alloc_array<Blob>(max))                ||
            !(object_types        = alloc_array<hxhim_object_type_t>(max)) ||
            !(orig.subjects       = alloc_array<void *>(max))              ||
            !(orig.predicates     = alloc_array<void *>(max)))              {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BGet::steal(Transport::Request::BGet *from, const std::size_t i) {
    if (Request::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    subjects[count]           = from->subjects[i];
    predicates[count]         = from->predicates[i];
    object_types[count]       = from->object_types[i];
    orig.subjects[count]      = from->orig.subjects[i];
    orig.predicates[count]    = from->orig.predicates[i];
    count++;

    from->orig.subjects[i]    = nullptr;
    from->orig.predicates[i]  = nullptr;

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BGet::cleanup() {
    dealloc_array(subjects, max_count);
    subjects = nullptr;

    dealloc_array(predicates, max_count);
    predicates = nullptr;

    dealloc_array(object_types, max_count);
    object_types = nullptr;

    dealloc_array(orig.subjects, max_count);
    orig.subjects = nullptr;

    dealloc_array(orig.predicates, max_count);
    orig.predicates = nullptr;

    return Request::cleanup();
}

Transport::Response::BGet::BGet(const std::size_t max)
    : Response(hxhim_op_t::HXHIM_GET),
      object_types(nullptr),
      objects(nullptr),
      orig()
{
    alloc(max);
}

Transport::Response::BGet::~BGet() {
    cleanup();
}

std::size_t Transport::Response::BGet::size() const {
    std::size_t total = Response::size();
    for(std::size_t i = 0; i < count; i++) {
        total += orig.subjects[i].pack_ref_size() +
                 orig.predicates[i].pack_ref_size() +
                 sizeof(object_types[i]);

        if (statuses[i] == TRANSPORT_SUCCESS) {
            total += objects[i].pack_size();
        }
    }

    return total;
}

int Transport::Response::BGet::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Response::alloc(max) != TRANSPORT_SUCCESS)                     ||
            !(object_types         = alloc_array<hxhim_object_type_t>(max)) ||
            !(objects              = alloc_array<Blob>(max))                ||
            !(orig.subjects        = alloc_array<Blob>(max))                ||
            !(orig.predicates      = alloc_array<Blob>(max)))                {
            cleanup();
            return TRANSPORT_SUCCESS;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BGet::steal(Transport::Response::BGet *bget, const std::size_t i) {
    if (Response::steal(bget, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    object_types[count]       = bget->object_types[i];
    objects[count]            = bget->objects[i];
    orig.subjects[count]      = bget->orig.subjects[i];
    orig.predicates[count]    = bget->orig.predicates[i];

    count++;

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BGet::cleanup() {
    dealloc_array(object_types, max_count);
    object_types = nullptr;

    dealloc_array(objects, max_count);
    objects = nullptr;

    // the pointers in these arrays don't belong to the server
    dealloc_array(orig.subjects, max_count);
    orig.subjects = nullptr;

    dealloc_array(orig.predicates, max_count);
    orig.predicates = nullptr;

    return Response::cleanup();
}
