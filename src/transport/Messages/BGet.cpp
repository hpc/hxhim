#include "transport/Messages/BGet.hpp"

Transport::Request::BGet::BGet(const std::size_t max)
    : Request(BGET),
      subjects(nullptr),
      predicates(nullptr),
      object_types(nullptr)
{
    alloc(max);
}

Transport::Request::BGet::~BGet() {
    cleanup();
}

std::size_t Transport::Request::BGet::size() const {
    std::size_t total = Request::size();
    for(std::size_t i = 0; i < count; i++) {
        total += subjects[i]->len + sizeof(subjects[i]->len) +
                 predicates[i]->len + sizeof(predicates[i]->len) +
                 sizeof(object_types[i]);
    }
    return total;
}

int Transport::Request::BGet::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Request::alloc(max) != TRANSPORT_SUCCESS)        ||
            !(subjects = alloc_array<Blob *>(max))            ||
            !(predicates = alloc_array<Blob *>(max))          ||
            !(object_types = alloc_array<hxhim_type_t>(max)))  {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BGet::cleanup() {
    for(std::size_t i = 0; i < count; i++) {
        destruct(subjects[i]);
        destruct(predicates[i]);
    }

    dealloc_array(subjects, count);
    subjects = nullptr;

    dealloc_array(predicates, count);
    predicates = nullptr;

    dealloc_array(object_types, count);
    object_types = nullptr;

    return Request::cleanup();
}

Transport::Response::BGet::BGet(const std::size_t max)
    : Response(BGET),
      subjects(nullptr),
      predicates(nullptr),
      object_types(nullptr),
      objects(nullptr),
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
                 sizeof(object_types[i]);

        if (statuses[i] == TRANSPORT_SUCCESS) {
            total += objects[i]->len + sizeof(objects[i]->len);
        }
    }
    return total;
}

int Transport::Response::BGet::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Response::alloc(max) != TRANSPORT_SUCCESS)       ||
            !(subjects = alloc_array<Blob *>(max))            ||
            !(predicates = alloc_array<Blob *>(max))          ||
            !(object_types = alloc_array<hxhim_type_t>(max))  ||
            !(objects = alloc_array<Blob *>(max)))             {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BGet::merge(Transport::Response::BGet *bget, const int ds) {
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

        // remove ownership
        bget->subjects[i] = nullptr;
        bget->predicates[i] = nullptr;
        bget->objects[i] = nullptr;

        count++;
    }

    bget->count = 0;

    return HXHIM_SUCCESS;
}

int Transport::Response::BGet::cleanup() {
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

    return Response::cleanup();
}
