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

int Transport::Request::BGetOp::steal(Transport::Request::BGetOp *from, const std::size_t i) {
    if (Request::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    subjects[count]          = from->subjects[i];
    predicates[count]        = from->predicates[i];
    object_types[count]      = from->object_types[i];
    num_recs[count]          = from->num_recs[i];
    ops[count]               = from->ops[i];

    count++;

    from->subjects[i]        = nullptr;
    from->predicates[i]      = nullptr;

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BGetOp::cleanup() {
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
                 sizeof(object_types[i]);

        if (statuses[i] == TRANSPORT_SUCCESS) {
            total += objects[i]->len + sizeof(objects[i]->len);
        }
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

int Transport::Response::BGetOp::steal(Transport::Response::BGetOp *bget, const std::size_t i) {
    if (Response::steal(bget, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    subjects[count] = bget->subjects[i];
    predicates[count] = bget->predicates[i];
    object_types[count] = bget->object_types[i];
    objects[count] = bget->objects[i];

    // remove ownership
    bget->subjects[i] = nullptr;
    bget->predicates[i] = nullptr;
    bget->object_types[i] = HXHIM_INVALID_TYPE;
    bget->objects[i] = nullptr;

    count++;

    return HXHIM_SUCCESS;
}

int Transport::Response::BGetOp::cleanup() {
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
