#include "transport/Messages/BGet.hpp"

Transport::Request::BGet::BGet(const std::size_t max)
    : Request(BGET),
      subjects(nullptr),
      subject_lens(nullptr),
      predicates(nullptr),
      predicate_lens(nullptr),
      object_types(nullptr)
{
    alloc(max);
}

Transport::Request::BGet::~BGet() {
    cleanup();
}

std::size_t Transport::Request::BGet::size() const {
    std::size_t total = Request::size() + sizeof(count);
    for(std::size_t i = 0; i < count; i++) {
        total += sizeof(ds_offsets[i]) +
            subject_lens[i] + sizeof(subject_lens[i]) +
            predicate_lens[i] + sizeof(predicate_lens[i]) +
            sizeof(object_types[i]);
    }
    return total;
}

int Transport::Request::BGet::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Message::alloc(max) != TRANSPORT_SUCCESS)           ||
            !(subjects = alloc_array<void *>(max))            ||
            !(subject_lens = alloc_array<std::size_t>(max))   ||
            !(predicates = alloc_array<void *>(max))          ||
            !(predicate_lens = alloc_array<std::size_t>(max)) ||
            !(object_types = alloc_array<hxhim_type_t>(max)))  {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BGet::cleanup() {
    if (clean) {
        if (subjects) {
            for(std::size_t i = 0; i < count; i++) {
                dealloc(subjects[i]);
            }
        }

        if (predicates) {
            for(std::size_t i = 0; i < count; i++) {
                dealloc(predicates[i]);
            }
        }
    }

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

    return TRANSPORT_SUCCESS;
}

Transport::Response::BGet::BGet(const std::size_t max)
    : Response(BGET),
      statuses(nullptr),
      subjects(nullptr),
      subject_lens(nullptr),
      predicates(nullptr),
      predicate_lens(nullptr),
      object_types(nullptr),
      objects(nullptr),
      object_lens(nullptr),
      next(nullptr)
{
    alloc(max);
}

Transport::Response::BGet::~BGet() {
    cleanup();
}

std::size_t Transport::Response::BGet::size() const {
    std::size_t total = Response::size() + sizeof(count);
    for(std::size_t i = 0; i < count; i++) {
        total += sizeof(ds_offsets[i]) +
            sizeof(statuses[i]) +
            subject_lens[i] + sizeof(subject_lens[i]) +
            predicate_lens[i] + sizeof(predicate_lens[i]) +
            sizeof(object_types[i]) + object_lens[i] + sizeof(object_lens[i]);
    }
    return total;
}

int Transport::Response::BGet::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Message::alloc(max) != TRANSPORT_SUCCESS)           ||
            !(statuses = alloc_array<int>(max))               ||
            !(subjects = alloc_array<void *>(max))            ||
            !(subject_lens = alloc_array<std::size_t>(max))   ||
            !(predicates = alloc_array<void *>(max))          ||
            !(predicate_lens = alloc_array<std::size_t>(max)) ||
            !(object_types = alloc_array<hxhim_type_t>(max))  ||
            !(objects = alloc_array<void *>(max))             ||
            !(object_lens = alloc_array<std::size_t>(max)))    {
            cleanup();
            return TRANSPORT_SUCCESS;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BGet::cleanup() {
    if (clean) {
        if (subjects) {
            for(std::size_t i = 0; i < count; i++) {
                dealloc(subjects[i]);
            }
        }

        if (predicates) {
            for(std::size_t i = 0; i < count; i++) {
                dealloc(predicates[i]);
            }
        }

        if (objects) {
            for(std::size_t i = 0; i < count; i++) {
                dealloc(objects[i]);
            }
        }
    }

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

    return TRANSPORT_SUCCESS;
}
