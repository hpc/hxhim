#include "transport/BGetOp.hpp"

Transport::Request::BGetOp::BGetOp(const std::size_t max)
    : Request(BGETOP),
      Bulk(),
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
    std::size_t total = Request::size() + sizeof(count);
    for(std::size_t i = 0; i < count; i++) {
        total += sizeof(db_offsets[i]) +
            subject_lens[i] + sizeof(subject_lens[i]) +
            predicate_lens[i] + sizeof(predicate_lens[i]) +
            sizeof(object_types[i]) +
            sizeof(num_recs[i]) + sizeof(ops[i]);
    }
    return total;
}

int Transport::Request::BGetOp::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Bulk::alloc(max) != TRANSPORT_SUCCESS)  ||
            !(subjects = new void *[max])            ||
            !(subject_lens = new std::size_t[max])   ||
            !(predicates = new void *[max])          ||
            !(predicate_lens = new std::size_t[max]) ||
            !(object_types = new hxhim_type_t[max])  ||
            !(num_recs = new std::size_t[max])       ||
            !(ops = new hxhim_get_op_t[max]))         {
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
                ::operator delete(subjects[i]);
            }
        }

        if (predicates) {
            for(std::size_t i = 0; i < count; i++) {
                ::operator delete(predicates[i]);
            }
        }
    }

    delete [] subjects;
    subjects = nullptr;

    delete [] subject_lens;
    subject_lens = nullptr;

    delete [] predicates;
    predicates = nullptr;

    delete [] predicate_lens;
    predicate_lens = nullptr;

    delete [] object_types;
    object_types = nullptr;

    delete [] num_recs;
    num_recs = nullptr;

    delete [] ops;
    ops = nullptr;

    Bulk::cleanup();

    return TRANSPORT_SUCCESS;
}

Transport::Response::BGetOp::BGetOp(const std::size_t max)
    : Response(BGETOP),
      Bulk(),
      statuses(nullptr),
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
    std::size_t total = Response::size() + sizeof(count);
    for(std::size_t i = 0; i < count; i++) {
        total += sizeof(db_offsets[i]) +
            sizeof(statuses[i]) +
            subject_lens[i] + sizeof(subject_lens[i]) +
            predicate_lens[i] + sizeof(predicate_lens[i]) +
            sizeof(object_types[i]) + object_lens[i] + sizeof(object_lens[i]);
    }
    return total;
}

int Transport::Response::BGetOp::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        statuses = new int[max];
        subjects = new void *[max];
        subject_lens = new std::size_t[max];
        predicates = new void *[max];
        predicate_lens = new std::size_t[max];
        object_types = new hxhim_type_t[max];
        objects = new void *[max];
        object_lens = new std::size_t[max];
        Bulk::alloc(max);
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BGetOp::cleanup() {
    if (clean) {
        if (subjects) {
            for(std::size_t i = 0; i < count; i++) {
                ::operator delete(subjects[i]);
            }
        }

        if (predicates) {
            for(std::size_t i = 0; i < count; i++) {
                ::operator delete(predicates[i]);
            }
        }

        if (objects) {
            for(std::size_t i = 0; i < count; i++) {
                ::operator delete(objects[i]);
            }
        }
    }

    delete [] statuses;
    statuses = nullptr;

    delete [] subjects;
    subjects = nullptr;

    delete [] subject_lens;
    subject_lens = nullptr;

    delete [] predicates;
    predicates = nullptr;

    delete [] predicate_lens;
    predicate_lens = nullptr;

    delete [] object_types;
    object_types = nullptr;

    delete [] objects;
    objects = nullptr;

    delete [] object_lens;
    object_lens = nullptr;

    delete next;
    next = nullptr;

    Bulk::cleanup();

    return TRANSPORT_SUCCESS;
}
