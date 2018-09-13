#include "transport/Messages/BPut.hpp"

Transport::Request::BPut::BPut(FixedBufferPool *arrays, FixedBufferPool *buffers, const std::size_t max)
    : Request(BPUT, arrays, buffers),
      Bulk(),
      subjects(nullptr),
      subject_lens(nullptr),
      predicates(nullptr),
      predicate_lens(nullptr),
      object_types(nullptr),
      objects(nullptr),
      object_lens(nullptr)
{
    alloc(max);
}

Transport::Request::BPut::~BPut() {
    cleanup();
}

std::size_t Transport::Request::BPut::size() const {
    std::size_t total = Request::size() + sizeof(count);
    for(std::size_t i = 0; i < count; i++) {
        total += sizeof(ds_offsets[i]) +
            subject_lens[i] + sizeof(subject_lens[i]) +
            predicate_lens[i] + sizeof(predicate_lens[i]) +
            sizeof(object_types[i]) + object_lens[i] + sizeof(object_lens[i]);
    }
    return total;
}

int Transport::Request::BPut::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Bulk::alloc(max, arrays) != TRANSPORT_SUCCESS)             ||
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

int Transport::Request::BPut::cleanup() {
    if (clean) {
        if (subjects) {
            for(std::size_t i = 0; i < count; i++) {
                buffers->release(subjects[i], subject_lens[i]);
            }
        }

        if (predicates) {
            for(std::size_t i = 0; i < count; i++) {
                buffers->release(predicates[i], predicate_lens[i]);
            }
        }

        if (objects) {
            for(std::size_t i = 0; i < count; i++) {
                buffers->release(objects[i], object_lens[i]);
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

    arrays->release_array(objects, count);
    objects = nullptr;

    arrays->release_array(object_lens, count);
    object_lens = nullptr;

    Bulk::cleanup(arrays);

    return TRANSPORT_SUCCESS;
}

Transport::Response::BPut::BPut(FixedBufferPool *arrays, FixedBufferPool *buffers, const std::size_t max)
    : Response(Message::BPUT, arrays, buffers),
      Bulk(),
      statuses(nullptr),
      next(nullptr)
{
    alloc(max);
}

Transport::Response::BPut::~BPut() {
    cleanup();
}

std::size_t Transport::Response::BPut::size() const {
    return Response::size() + sizeof(count) + (sizeof(*ds_offsets) * count) + (sizeof(*statuses) * count);
}

int Transport::Response::BPut::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Bulk::alloc(max, arrays) != TRANSPORT_SUCCESS) ||
            !(statuses = arrays->acquire_array<int>(max)))   {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BPut::cleanup() {
    arrays->release_array(statuses, count);
    statuses = nullptr;

    Bulk::cleanup(arrays);

    return TRANSPORT_SUCCESS;
}
