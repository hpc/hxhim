#include "transport/Messages/BGetOp.hpp"

Transport::Request::BGetOp::BGetOp(const std::size_t max)
    : Request(BGETOP),
      subjects(nullptr),
      subject_lens(nullptr),
      predicates(nullptr),
      predicate_lens(nullptr),
      object_types(nullptr),
      num_recs(nullptr),
      ops(nullptr)
{
    alloc(max);
}

Transport::Request::BGetOp::~BGetOp() {
    cleanup();
}

std::size_t Transport::Request::BGetOp::size() const {
    std::size_t total = Request::size();
    for(std::size_t i = 0; i < count; i++) {
        total += subject_lens[i] + sizeof(subject_lens[i]) +
                 predicate_lens[i] + sizeof(predicate_lens[i]) +
                 sizeof(object_types[i]) +
                 sizeof(num_recs[i]) + sizeof(ops[i]);
    }
    return total;
}

int Transport::Request::BGetOp::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Request::alloc(max) != TRANSPORT_SUCCESS)        ||
            !(subjects = alloc_array<void *>(max))            ||
            !(subject_lens = alloc_array<std::size_t>(max))   ||
            !(predicates = alloc_array<void *>(max))          ||
            !(predicate_lens = alloc_array<std::size_t>(max)) ||
            !(object_types = alloc_array<hxhim_type_t>(max))  ||
            !(num_recs = alloc_array<std::size_t>(max))       ||
            !(ops = alloc_array<hxhim_get_op_t>(max)))         {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BGetOp::cleanup() {
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

    dealloc_array(num_recs, count);
    num_recs = nullptr;

    dealloc_array(ops, count);
    ops = nullptr;

    return Request::cleanup();
}

Transport::Response::BGetOp::BGetOp(const std::size_t max)
    : Response(BGETOP),
      subjects(nullptr),
      subject_lens(nullptr),
      predicates(nullptr),
      predicate_lens(nullptr),
      object_types(nullptr),
      objects(nullptr),
      object_lens(0),
      next(nullptr)
{
    alloc(max);
}

Transport::Response::BGetOp::~BGetOp() {
    cleanup();
}

std::size_t Transport::Response::BGetOp::size() const {
    std::size_t total = Response::size();
    for(std::size_t i = 0; i < count; i++) {
        total += subject_lens[i] + sizeof(subject_lens[i]) +
                 predicate_lens[i] + sizeof(predicate_lens[i]) +
                 sizeof(object_types[i]) + object_lens[i] + sizeof(object_lens[i]);
    }
    return total;
}

int Transport::Response::BGetOp::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Response::alloc(max) != TRANSPORT_SUCCESS)       ||
            !(subjects = alloc_array<void *>(max))            ||
            !(subject_lens = alloc_array<std::size_t>(max))   ||
            !(predicates = alloc_array<void *>(max))          ||
            !(predicate_lens = alloc_array<std::size_t>(max)) ||
            !(object_types = alloc_array<hxhim_type_t>(max))  ||
            !(objects = alloc_array<void *>(max))             ||
            !(object_lens = alloc_array<std::size_t>(max)))    {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BGetOp::cleanup() {
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

    return Response::cleanup();
}
