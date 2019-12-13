#include "transport/Messages/BGet2.hpp"
#include <cstdio>

Transport::Request::BGet2::BGet2(const std::size_t max)
    : Request(BGET2),
      subjects(nullptr),
      subject_lens(nullptr),
      predicates(nullptr),
      predicate_lens(nullptr),
      object_types(nullptr),
      objects(nullptr),
      object_lens(nullptr),
      orig()
{
    alloc(max);
}

Transport::Request::BGet2::~BGet2() {
    cleanup();
}

std::size_t Transport::Request::BGet2::size() const {
    std::size_t total = Request::size() + sizeof(count);
    for(std::size_t i = 0; i < count; i++) {
        total += sizeof(ds_offsets[i]) +
            subject_lens[i] + sizeof(subject_lens[i]) +
            predicate_lens[i] + sizeof(predicate_lens[i]) +
            sizeof(object_types[i]) + sizeof(objects[i]) + sizeof(object_lens[i]) +
            sizeof(orig.subjects[i]) +
            sizeof(orig.predicates[i]) +
            sizeof(orig.objects[i]) + sizeof(orig.object_lens[i]);

    }
    return total;
}

int Transport::Request::BGet2::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Message::alloc(max) != TRANSPORT_SUCCESS)                  ||
            !(subjects = alloc_array<void *>(max))                   ||
            !(subject_lens = alloc_array<std::size_t>(max))          ||
            !(predicates = alloc_array<void *>(max))                 ||
            !(predicate_lens = alloc_array<std::size_t>(max))        ||
            !(object_types = alloc_array<hxhim_type_t>(max))         ||
            !(objects = alloc_array<void *>(max))                    ||
            !(object_lens = alloc_array<std::size_t *>(max))         ||
            !(orig.subjects = alloc_array<void *>(max))              ||
            !(orig.predicates = alloc_array<void *>(max))            ||
            !(orig.objects = alloc_array<void *>(max))               ||
            !(orig.object_lens = alloc_array<std::size_t *>(max)))    {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BGet2::cleanup() {
    dealloc_array(subjects, count);
    subjects = nullptr;

    dealloc_array(subject_lens, count);
    subject_lens = nullptr;

    dealloc_array(predicates, count);
    predicates = nullptr;

    dealloc_array(predicate_lens, count);
    predicate_lens = nullptr;

    dealloc_array(object_types, count);
    object_types = nullptr;

    dealloc_array(objects, count);
    objects = nullptr;

    dealloc_array(object_lens, count);
    object_lens = nullptr;

    dealloc_array(orig.subjects, count);
    orig.subjects = nullptr;

    dealloc_array(orig.predicates, count);
    orig.predicates = nullptr;

    dealloc_array(orig.objects, count);
    orig.objects = nullptr;

    dealloc_array(orig.object_lens, count);
    orig.object_lens = nullptr;

    return TRANSPORT_SUCCESS;
}

Transport::Response::BGet2::BGet2(const std::size_t max)
    : Response(BGET2),
      statuses(nullptr),
      subjects(nullptr),
      subject_lens(nullptr),
      predicates(nullptr),
      predicate_lens(nullptr),
      object_types(nullptr),
      objects(nullptr),
      object_lens(nullptr),
      orig(),
      next(nullptr)
{
    alloc(max);
}

Transport::Response::BGet2::~BGet2() {
    cleanup();
}

std::size_t Transport::Response::BGet2::size() const {
    std::size_t total = Response::size() + sizeof(count);
    for(std::size_t i = 0; i < count; i++) {
        total += sizeof(ds_offsets[i]) +
            sizeof(statuses[i]) +
            sizeof(subject_lens[i]) +
            sizeof(predicate_lens[i]) +
            sizeof(object_types[i]) + *(object_lens[i]) + sizeof((*object_lens)[i]) +
            sizeof(orig.subjects[i]) +
            sizeof(orig.predicates[i]) +
            sizeof(orig.objects[i]) + sizeof(orig.object_lens[i]);
    }

    if (subjects) {
        for(std::size_t i = 0; i < count; i++) {
            total += sizeof(subjects[i]) + sizeof(subject_lens[i]);
        }
    }

    if (predicates) {
        for(std::size_t i = 0; i < count; i++) {
            total += sizeof(predicates[i]) + sizeof(predicate_lens[i]);
        }
    }

    return total;
}

int Transport::Response::BGet2::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Message::alloc(max) != TRANSPORT_SUCCESS)                  ||
            !(statuses = alloc_array<int>(max))                      ||
            !(subjects = alloc_array<void *>(max))                   ||
            !(subject_lens = alloc_array<std::size_t>(max))          ||
            !(predicates = alloc_array<void *>(max))                 ||
            !(predicate_lens = alloc_array<std::size_t>(max))        ||
            !(object_types = alloc_array<hxhim_type_t>(max))         ||
            !(objects = alloc_array<void *>(max))                    ||
            !(object_lens = alloc_array<std::size_t *>(max))         ||
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
        subject_lens[count] = bget->subject_lens[i];
        predicates[count] = bget->predicates[i];
        predicate_lens[count] = bget->predicate_lens[i];
        object_types[count] = bget->object_types[i];
        objects[count] = bget->objects[i];
        object_lens[count] = bget->object_lens[i];
        orig.subjects[count] = bget->orig.subjects[i];
        orig.predicates[count] = bget->orig.predicates[i];
        orig.objects[count] = bget->orig.objects[i];
        orig.object_lens[count] = bget->orig.object_lens[i];

        // remove ownership
        bget->subjects[i] = nullptr;
        bget->subject_lens[i] = 0;
        bget->predicates[i] = nullptr;
        bget->predicate_lens[i] = 0;
        bget->objects[i] = nullptr;
        bget->object_lens[i] = nullptr;
        bget->orig.subjects[i] = nullptr;
        bget->orig.predicates[i] = nullptr;
        bget->orig.objects[i] = nullptr;
        bget->orig.object_lens[i] = nullptr;

        count++;
    }

    return HXHIM_SUCCESS;
}

int Transport::Response::BGet2::cleanup() {
    dealloc_array(statuses, count);
    statuses = nullptr;

    dealloc_array(subjects, count);
    subjects = nullptr;

    dealloc_array(subject_lens, count);
    subject_lens = nullptr;

    dealloc_array(predicates, count);
    predicates = nullptr;

    dealloc_array(predicate_lens, count);
    predicate_lens = nullptr;

    dealloc_array(object_types, count);
    object_types = nullptr;

    dealloc_array(objects, count);
    objects = nullptr;

    dealloc_array(object_lens, count);
    object_lens = nullptr;

    // the pointers in these arrays don't belong to the server
    dealloc_array(orig.subjects, count);
    orig.subjects = nullptr;

    dealloc_array(orig.predicates, count);
    orig.predicates = nullptr;

    dealloc_array(orig.objects, count);
    orig.objects = nullptr;

    dealloc_array(orig.object_lens, count);
    orig.object_lens = nullptr;

    return TRANSPORT_SUCCESS;
}

void Transport::Response::BGet2::server_side_cleanup(void *) {
    // only clean up server side copies of the SPO, not the client side
    dealloc_array(subject_lens, count);
    dealloc_array(predicate_lens, count);
    for(std::size_t i = 0; i < count; i++) {
        dealloc(subjects[i]);
        dealloc(predicates[i]);
        dealloc(objects[i]);
        destruct(object_lens[i]);
    }
}
