#include "transport/Messages/BGet.hpp"
#include <cstdio>

Transport::Request::BGet::BGet(const std::size_t max)
    : Request(BGET),
      subjects(nullptr),
      predicates(nullptr),
      object_types(nullptr),
      objects(nullptr),
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
        total += subjects[i]->len + sizeof(subjects[i]->len) + sizeof(orig.subjects[i]) +
                 predicates[i]->len + sizeof(predicates[i]->len) + sizeof(orig.predicates[i]) +
                 sizeof(object_types[i]) + sizeof(objects[i]->ptr) + sizeof(objects[i]->len);
    }
    return total;
}

int Transport::Request::BGet::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Request::alloc(max) != TRANSPORT_SUCCESS)        ||
            !(subjects = alloc_array<Blob *>(max))            ||
            !(predicates = alloc_array<Blob *>(max))          ||
            !(object_types = alloc_array<hxhim_type_t>(max))  ||
            !(objects = alloc_array<Blob *>(max))             ||
            !(orig.subjects = alloc_array<void *>(max))       ||
            !(orig.predicates = alloc_array<void *>(max))     ||
            !(orig.objects = alloc_array<void *>(max))        ||
            !(orig.object_lens = alloc_array<std::size_t *>(max)))    {
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
    objects[count]            = from->objects[i];
    orig.subjects[count]      = from->orig.subjects[i];
    orig.predicates[count]    = from->orig.predicates[i];
    orig.objects[count]       = from->orig.objects[i];
    orig.object_lens[count]   = from->orig.object_lens[i];
    count++;

    from->subjects[i]         = nullptr;
    from->predicates[i]       = nullptr;
    from->objects[i]          = nullptr;
    from->orig.subjects[i]    = nullptr;
    from->orig.predicates[i]  = nullptr;
    from->orig.objects[i]     = nullptr;
    from->orig.object_lens[i] = nullptr;

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BGet::cleanup() {
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

    dealloc_array(orig.objects, count);
    orig.objects = nullptr;

    dealloc_array(orig.object_lens, count);
    orig.object_lens = nullptr;

    return Request::cleanup();
}

Transport::Response::BGet::BGet(const std::size_t max)
    : Response(BGET),
      subjects(nullptr),
      predicates(nullptr),
      object_types(nullptr),
      objects(nullptr),
      orig(),
      next(nullptr)
{
    alloc(max);
}

Transport::Response::BGet::~BGet() {
    cleanup();
}

std::size_t Transport::Response::BGet::size() const {
    std::size_t total = Response::size();
    for(std::size_t i = 0; i < count; i++) {
        total += subjects[i]->len + sizeof(subjects[i]->len) +
                 predicates[i]->len + sizeof(predicates[i]->len) +
                 sizeof(object_types[i]) +
                 sizeof(orig.subjects[i]) +
                 sizeof(orig.predicates[i]) +
                 sizeof(orig.objects[i]) + sizeof(orig.object_lens[i]);

        if (statuses[i] == TRANSPORT_SUCCESS) {
            total += objects[i]->len + sizeof(objects[i]->len);
        }
    }

    return total;
}

int Transport::Response::BGet::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Response::alloc(max) != TRANSPORT_SUCCESS)              ||
            !(subjects = alloc_array<Blob *>(max))                   ||
            !(predicates = alloc_array<Blob *>(max))                 ||
            !(object_types = alloc_array<hxhim_type_t>(max))         ||
            !(objects = alloc_array<Blob *>(max))                    ||
            !(orig.subjects = alloc_array<void *>(max))              ||
            !(orig.predicates = alloc_array<void *>(max))            ||
            !(orig.objects = alloc_array<void *>(max))               ||
            !(orig.object_lens = alloc_array<std::size_t *>(max)))    {
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

    subjects[count]           = bget->subjects[i];
    predicates[count]         = bget->predicates[i];
    object_types[count]       = bget->object_types[i];
    objects[count]            = bget->objects[i];
    orig.subjects[count]      = bget->orig.subjects[i];
    orig.predicates[count]    = bget->orig.predicates[i];
    orig.objects[count]       = bget->orig.objects[i];
    orig.object_lens[count]   = bget->orig.object_lens[i];
    count++;

    // remove ownership
    bget->subjects[i]         = nullptr;
    bget->predicates[i]       = nullptr;
    bget->objects[i]          = nullptr;
    bget->orig.subjects[i]    = nullptr;
    bget->orig.predicates[i]  = nullptr;
    bget->orig.objects[i]     = nullptr;
    bget->orig.object_lens[i] = nullptr;

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BGet::cleanup() {
    dealloc_array(subjects, count);
    subjects = nullptr;

    dealloc_array(predicates, count);
    predicates = nullptr;

    dealloc_array(object_types, count);
    object_types = nullptr;

    dealloc_array(objects, count);
    objects = nullptr;

    // the pointers in these arrays don't belong to the server
    dealloc_array(orig.subjects, count);
    orig.subjects = nullptr;

    dealloc_array(orig.predicates, count);
    orig.predicates = nullptr;

    dealloc_array(orig.objects, count);
    orig.objects = nullptr;

    dealloc_array(orig.object_lens, count);
    orig.object_lens = nullptr;

    return Response::cleanup();
}
