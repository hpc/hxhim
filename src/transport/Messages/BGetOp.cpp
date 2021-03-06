#include "transport/Messages/BGetOp.hpp"

Transport::Request::BGetOp::BGetOp(const std::size_t max)
    : SubjectPredicate(hxhim_op_t::HXHIM_GETOP),
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
    std::size_t total = Request::size(); // do not call SubjectPredicate::size()
    for(std::size_t i = 0; i < count; i++) {
        total += sizeof(object_types[i]) +
                 sizeof(num_recs[i]) + sizeof(ops[i]);

        if ((ops[i] != hxhim_getop_t::HXHIM_GETOP_FIRST) &&
            (ops[i] != hxhim_getop_t::HXHIM_GETOP_LAST)) {
            total += subjects[i].pack_size(true) +
                     predicates[i].pack_size(true);
        }
    }
    return total;
}

void Transport::Request::BGetOp::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        SubjectPredicate::alloc(max);
        object_types = alloc_array<hxhim_data_t>(max);
        num_recs     = alloc_array<std::size_t>(max);
        ops          = alloc_array<hxhim_getop_t>(max);
    }
}

int Transport::Request::BGetOp::steal(Transport::Request::BGetOp *from, const std::size_t i) {
    if (SubjectPredicate::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    object_types[count] = from->object_types[i];
    num_recs[count]     = from->num_recs[i];
    ops[count]          = from->ops[i];

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

    return SubjectPredicate::cleanup();
}

Transport::Response::BGetOp::BGetOp(const std::size_t max)
    : Response(hxhim_op_t::HXHIM_GETOP),
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
        total += sizeof(num_recs[i]);
        for(std::size_t j = 0; j < num_recs[i]; j++) {
            total += subjects[i][j].pack_size(true) +
                     predicates[i][j].pack_size(true);

            // all records from response[i] share the same status
            if (statuses[i] == DATASTORE_SUCCESS) {
                total += objects[i][j].pack_size(true);
            }
        }
    }
    return total;
}

void Transport::Response::BGetOp::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        Response::alloc(max);
        num_recs     = alloc_array<std::size_t>(max);
        subjects     = alloc_array<Blob *>(max);
        predicates   = alloc_array<Blob *>(max);
        objects      = alloc_array<Blob *>(max);
    }
}

int Transport::Response::BGetOp::steal(Transport::Response::BGetOp *from, const std::size_t i) {
    if (Response::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    num_recs[count]     = from->num_recs[i];
    subjects[count]     = std::move(from->subjects[i]);
    predicates[count]   = std::move(from->predicates[i]);
    objects[count]      = std::move(from->objects[i]);

    count++;

    return HXHIM_SUCCESS;
}

int Transport::Response::BGetOp::cleanup() {
    for(std::size_t i = 0; i < count; i++) {
        dealloc_array(subjects[i],   num_recs[i]);
        dealloc_array(predicates[i], num_recs[i]);
        dealloc_array(objects[i],    num_recs[i]);
    }

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
