#include "transport/Messages/BGet2.hpp"
#include <cstdio>

Transport::Request::BGet2::BGet2(const std::size_t max)
    : Request(BGET2),
      subjects(nullptr),
      predicates(nullptr),
      object_types(nullptr),
      objects(nullptr),
      orig()
{
    alloc(max);
}

Transport::Request::BGet2::~BGet2() {
    cleanup();
}

std::size_t Transport::Request::BGet2::size() const {
    std::size_t total = Request::size();
    for(std::size_t i = 0; i < count; i++) {
        total += subjects[i]->len + sizeof(subjects[i]->len) + sizeof(orig.subjects[i]) +
                 predicates[i]->len + sizeof(predicates[i]->len) + sizeof(orig.predicates[i]) +
                 sizeof(object_types[i]) + sizeof(objects[i]->ptr) + sizeof(objects[i]->len);
    }
    return total;
}

int Transport::Request::BGet2::alloc(const std::size_t max) {
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

int Transport::Request::BGet2::cleanup() {
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

    dealloc_array(orig.objects, count);
    orig.objects = nullptr;

    dealloc_array(orig.object_lens, count);
    orig.object_lens = nullptr;

    return Request::cleanup();
}

Transport::Response::BGet2::BGet2(const std::size_t max)
    : Response(BGET2),
      subjects(nullptr),
      predicates(nullptr),
      object_types(nullptr),
      objects(nullptr),
      orig(),
      next(nullptr)
{
    alloc(max);
}

Transport::Response::BGet2::~BGet2() {
    cleanup();
}

std::size_t Transport::Response::BGet2::size() const {
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

int Transport::Response::BGet2::alloc(const std::size_t max) {
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

int Transport::Response::BGet2::merge(Transport::Response::BGet2 *bget, const int ds) {
    if (!bget) {
        return HXHIM_ERROR;
    }

    // cannot copy all
    if ((count + bget->count) > max_count) {
        return HXHIM_ERROR;
    }

    for(std::size_t i = 0; i < bget->count; i++) {
        ds_offsets[count] = ds;
        statuses[count] = bget->statuses[i];
        subjects[count] = bget->subjects[i];
        predicates[count] = bget->predicates[i];
        object_types[count] = bget->object_types[i];
        objects[count] = bget->objects[i];
        orig.subjects[count] = bget->orig.subjects[i];
        orig.predicates[count] = bget->orig.predicates[i];
        orig.objects[count] = bget->orig.objects[i];
        orig.object_lens[count] = bget->orig.object_lens[i];

        // remove ownership
        bget->subjects[i] = nullptr;
        bget->predicates[i] = nullptr;
        bget->objects[i] = nullptr;
        bget->orig.subjects[i] = nullptr;
        bget->orig.predicates[i] = nullptr;
        bget->orig.objects[i] = nullptr;
        bget->orig.object_lens[i] = nullptr;

        count++;
    }

    bget->count = 0;

    return HXHIM_SUCCESS;
}

int Transport::Response::BGet2::cleanup() {
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
