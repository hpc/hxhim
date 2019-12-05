#include "transport/Messages/BGet2.hpp"
#include "utils/memory.hpp"

Transport::Request::BGet2::BGet2(const std::size_t max)
    : Request(BGET2),
      Bulk(),
      subjects(nullptr),
      subject_lens(nullptr),
      predicates(nullptr),
      predicate_lens(nullptr),
      object_types(nullptr),
      objects(nullptr),
      object_lens(nullptr),
      src_objects(nullptr),
      src_object_lens(nullptr)
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
            sizeof(object_types[i]) + sizeof(objects[i]) + sizeof(src_object_lens[i]);

    }
    return total;
}

int Transport::Request::BGet2::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Bulk::alloc(max) != TRANSPORT_SUCCESS)               ||
            !(subjects = alloc_array<void *>(max))                ||
            !(subject_lens = alloc_array<std::size_t>(max))       ||
            !(predicates = alloc_array<void *>(max))              ||
            !(predicate_lens = alloc_array<std::size_t>(max))     ||
            !(object_types = alloc_array<hxhim_type_t>(max))      ||
            !(objects = alloc_array<void *>(max))                 ||
            !(object_lens = alloc_array<std::size_t *>(max))      ||
            !(src_objects = alloc_array<void *>(max))             ||
            !(src_object_lens = alloc_array<std::size_t *>(max)))  {
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

    dealloc_array(src_objects, count);
    src_objects = nullptr;

    dealloc_array(src_object_lens, count);
    src_object_lens = nullptr;

    Bulk::cleanup();

    return TRANSPORT_SUCCESS;
}

Transport::Response::BGet2::BGet2(const std::size_t max)
    : Response(BGET2),
      Bulk(),
      statuses(nullptr),
      subjects(nullptr),
      subject_lens(nullptr),
      predicates(nullptr),
      predicate_lens(nullptr),
      object_types(nullptr),
      objects(nullptr),
      object_lens(nullptr),
      src_objects(nullptr),
      src_object_lens(nullptr),
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
            subject_lens[i] + sizeof(subject_lens[i]) +
            predicate_lens[i] + sizeof(predicate_lens[i]) +
            sizeof(object_types[i]) + *object_lens[i] + sizeof(*object_lens[i]) +
            sizeof(src_objects[i]) + sizeof(src_object_lens[i]);
    }

    return total;
}

int Transport::Response::BGet2::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Bulk::alloc(max) != TRANSPORT_SUCCESS)               ||
            !(statuses = alloc_array<int>(max))                   ||
            !(subjects = alloc_array<void *>(max))                ||
            !(subject_lens = alloc_array<std::size_t>(max))       ||
            !(predicates = alloc_array<void *>(max))              ||
            !(predicate_lens = alloc_array<std::size_t>(max))     ||
            !(object_types = alloc_array<hxhim_type_t>(max))      ||
            !(objects = alloc_array<void *>(max))                 ||
            !(object_lens = alloc_array<std::size_t *>(max))      ||
            !(src_objects = alloc_array<void *>(max))             ||
            !(src_object_lens = alloc_array<std::size_t *>(max)))  {
            cleanup();
            return TRANSPORT_SUCCESS;
        }
    }

    return TRANSPORT_SUCCESS;
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

    dealloc_array(src_objects, count);
    src_objects = nullptr;

    dealloc_array(src_object_lens, count);
    src_object_lens = nullptr;

    Bulk::cleanup();

    return TRANSPORT_SUCCESS;
}

void Transport::Response::BGet2::server_side_cleanup(void *) {
    for(std::size_t i = 0; i < count; i++) {
        dealloc(subjects[i]);
        dealloc(predicates[i]);
        dealloc(objects[i]);
        destruct(object_lens[i]);
    }
}
