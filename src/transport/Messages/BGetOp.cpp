#include "transport/Messages/BGetOp.hpp"

Transport::Request::BGetOp::BGetOp(const std::size_t max)
    : Request(BGETOP),
      subjects(nullptr),
      predicates(nullptr),
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
        total += subjects[i]->len + sizeof(subjects[i]->len) +
                 predicates[i]->len + sizeof(predicates[i]->len) +
                 sizeof(object_types[i]) +
                 sizeof(num_recs[i]) + sizeof(ops[i]);
    }
    return total;
}

int Transport::Request::BGetOp::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Request::alloc(max) != TRANSPORT_SUCCESS)        ||
            !(subjects = alloc_array<Blob *>(max))            ||
            !(predicates = alloc_array<Blob *>(max))          ||
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

    dealloc_array(num_recs, count);
    num_recs = nullptr;

    dealloc_array(ops, count);
    ops = nullptr;

    return Request::cleanup();
}

Transport::Response::BGetOp::BGetOp(const std::size_t max)
    : Response(BGETOP),
      subjects(nullptr),
      predicates(nullptr),
      object_types(nullptr),
      objects(nullptr),
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
        total += subjects[i]->len + sizeof(subjects[i]->len) +
                 predicates[i]->len + sizeof(predicates[i]->len) +
                 sizeof(object_types[i]) + objects[i]->len + sizeof(objects[i]->len);
    }
    return total;
}

int Transport::Response::BGetOp::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Response::alloc(max) != TRANSPORT_SUCCESS)      ||
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

int Transport::Response::BGetOp::cleanup() {
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
