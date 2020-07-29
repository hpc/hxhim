#include "transport/Messages/BGetOp.hpp"

Transport::Request::BGetOp::BGetOp(const std::size_t max)
    : Request(hxhim_op_t::HXHIM_GETOP),
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
        total += sizeof(object_types[i]) +
                 sizeof(num_recs[i]) + sizeof(ops[i]);

        if ((ops[i] != hxhim_get_op_t::HXHIM_GET_FIRST) &&
            (ops[i] != hxhim_get_op_t::HXHIM_GET_LAST)) {
            total += subjects[i].pack_size() +
                     predicates[i].pack_size();
        }
    }
    return total;
}

int Transport::Request::BGetOp::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Request::alloc(max) != TRANSPORT_SUCCESS)                     ||
            !(subjects            = alloc_array<Blob>(max))                ||
            !(predicates          = alloc_array<Blob>(max))                ||
            !(object_types        = alloc_array<hxhim_object_type_t>(max)) ||
            !(num_recs            = alloc_array<std::size_t>(max))         ||
            !(ops                 = alloc_array<hxhim_get_op_t>(max)))      {
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

    subjects[count]           = from->subjects[i];
    predicates[count]         = from->predicates[i];
    object_types[count]       = from->object_types[i];
    num_recs[count]           = from->num_recs[i];
    ops[count]                = from->ops[i];

    count++;

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BGetOp::cleanup() {
    dealloc_array(subjects, max_count);
    subjects = nullptr;

    dealloc_array(predicates, max_count);
    predicates = nullptr;

    dealloc_array(object_types, max_count);
    object_types = nullptr;

    dealloc_array(num_recs, max_count);
    num_recs = nullptr;

    dealloc_array(ops, max_count);
    ops = nullptr;

    return Request::cleanup();
}

Transport::Response::BGetOp::BGetOp(const std::size_t max)
    : Response(hxhim_op_t::HXHIM_GETOP),
      object_types(nullptr),
      num_recs(nullptr),
      subjects(nullptr),
      predicates(nullptr),
      objects(nullptr)
{
    alloc(max);
}

Transport::Response::BGetOp::~BGetOp() {
    cleanup();
}

std::size_t Transport::Response::BGetOp::size() const {
    std::size_t total = Response::size();
    for(std::size_t i = 0; i < count; i++) {
        total += sizeof(object_types[i]) + sizeof(num_recs[i]);
        for(std::size_t j = 0; j < num_recs[i]; j++) {
            total += subjects[i][j].pack_size() +
                     predicates[i][j].pack_size();

            // all records from response[i] share the same status
            if (statuses[i] == TRANSPORT_SUCCESS) {
                total += objects[i][j].pack_size();
            }
        }
    }
    return total;
}

int Transport::Response::BGetOp::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Response::alloc(max) != TRANSPORT_SUCCESS)                     ||
            !(object_types         = alloc_array<hxhim_object_type_t>(max)) ||
            !(num_recs             = alloc_array<std::size_t>(max))         ||
            !(subjects             = alloc_array<Blob *>(max))              ||
            !(predicates           = alloc_array<Blob *>(max))              ||
            !(objects              = alloc_array<Blob *>(max)))             {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BGetOp::steal(Transport::Response::BGetOp *from, const std::size_t i) {
    if (Response::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    object_types[count]   = from->object_types[i];
    num_recs[count]       = from->num_recs[i];
    subjects[count]       = from->subjects[i];
    predicates[count]     = from->predicates[i];
    objects[count]        = from->objects[i];

    count++;

    return HXHIM_SUCCESS;
}

int Transport::Response::BGetOp::cleanup() {
    for(std::size_t i = 0; i < max_count; i++) {
        dealloc_array(subjects[i],   num_recs[i]);
        dealloc_array(predicates[i], num_recs[i]);
        dealloc_array(objects[i],    num_recs[i]);
    }

    dealloc_array(object_types, max_count);
    object_types = nullptr;

    dealloc_array(num_recs, max_count);
    num_recs = nullptr;

    dealloc_array(subjects, max_count);
    subjects = nullptr;

    dealloc_array(predicates, max_count);
    predicates = nullptr;

    dealloc_array(objects, max_count);
    objects = nullptr;

    return Response::cleanup();
}
