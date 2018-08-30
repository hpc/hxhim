#include "transport/Messages/BGetOp.hpp"

Transport::Request::BGetOp::BGetOp(FixedBufferPool *arrays, FixedBufferPool *buffers, const std::size_t max)
    : Request(BGETOP, arrays, buffers),
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
        total += sizeof(ds_offsets[i]) +
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
        if ((Bulk::alloc(max, arrays) != TRANSPORT_SUCCESS)             ||
            !(subjects = arrays->acquire_array<void *>(max))            ||
            !(subject_lens = arrays->acquire_array<std::size_t>(max))   ||
            !(predicates = arrays->acquire_array<void *>(max))          ||
            !(predicate_lens = arrays->acquire_array<std::size_t>(max)) ||
            !(object_types = arrays->acquire_array<hxhim_type_t>(max))  ||
            !(num_recs = arrays->acquire_array<std::size_t>(max))       ||
            !(ops = arrays->acquire_array<hxhim_get_op_t>(max)))         {
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
                buffers->release(subjects[i]);
            }
        }

        if (predicates) {
            for(std::size_t i = 0; i < count; i++) {
                buffers->release(predicates[i]);
            }
        }
    }

    arrays->release_array(subjects, count);
    subjects = nullptr;

    arrays->release_array(subject_lens, count);
    subject_lens = nullptr;

    arrays->release_array(predicates, count);
    predicates = nullptr;

    arrays->release_array(predicate_lens, count);
    predicate_lens = nullptr;

    arrays->release_array(object_types, count);
    object_types = nullptr;

    arrays->release_array(num_recs, count);
    num_recs = nullptr;

    arrays->release_array(ops, count);
    ops = nullptr;

    Bulk::cleanup(arrays);

    return TRANSPORT_SUCCESS;
}

Transport::Response::BGetOp::BGetOp(FixedBufferPool *arrays, FixedBufferPool *buffers, const std::size_t max)
    : Response(BGETOP, arrays, buffers),
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
        total += sizeof(ds_offsets[i]) +
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
        if ((Bulk::alloc(max, arrays) != TRANSPORT_SUCCESS)             ||
            !(statuses = arrays->acquire_array<int>(max))               ||
            !(subjects = arrays->acquire_array<void *>(max))            ||
            !(subject_lens = arrays->acquire_array<std::size_t>(max))   ||
            !(predicates = arrays->acquire_array<void *>(max))          ||
            !(predicate_lens = arrays->acquire_array<std::size_t>(max)) ||
            !(object_types = arrays->acquire_array<hxhim_type_t>(max))  ||
            !(objects = arrays->acquire_array<void *>(max))             ||
            !(object_lens = arrays->acquire_array<std::size_t>(max)))    {
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
                buffers->release(subjects[i]);
            }
        }

        if (predicates) {
            for(std::size_t i = 0; i < count; i++) {
                buffers->release(predicates[i]);
            }
        }

        if (objects) {
            for(std::size_t i = 0; i < count; i++) {
                buffers->release(objects[i]);
            }
        }
    }

    arrays->release_array(statuses, count);
    statuses = nullptr;

    arrays->release_array(subjects, count);
    subjects = nullptr;

    arrays->release_array(subject_lens, count);
    subject_lens = nullptr;

    arrays->release_array(predicates, count);
    predicates = nullptr;

    arrays->release_array(predicate_lens, count);
    predicate_lens = nullptr;

    arrays->release_array(object_types, count);
    object_types = nullptr;

    arrays->release_array(objects, count);
    objects = nullptr;

    arrays->release_array(object_lens, count);
    object_lens = nullptr;

    Bulk::cleanup(arrays);

    return TRANSPORT_SUCCESS;
}
