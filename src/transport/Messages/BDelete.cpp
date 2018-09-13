#include "transport/Messages/BDelete.hpp"

Transport::Request::BDelete::BDelete(FixedBufferPool *arrays, FixedBufferPool *buffers, const std::size_t max)
    : Request(BDELETE, arrays, buffers),
      Bulk(),
      subjects(nullptr),
      subject_lens(nullptr),
      predicates(nullptr),
      predicate_lens(nullptr)
{
    alloc(max);
}

Transport::Request::BDelete::~BDelete() {
    cleanup();
}

std::size_t Transport::Request::BDelete::size() const {
    std::size_t total = Request::size() + sizeof(count);
    for(std::size_t i = 0; i < count; i++) {
        total += sizeof(ds_offsets[i]) +
            subject_lens[i] + sizeof(subject_lens[i]) +
            predicate_lens[i] + sizeof(predicate_lens[i]);
    }

    return total;
}

int Transport::Request::BDelete::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Bulk::alloc(max, arrays) != TRANSPORT_SUCCESS)             ||
            !(subjects = arrays->acquire_array<void *>(max))            ||
            !(subject_lens = arrays->acquire_array<std::size_t>(max))   ||
            !(predicates = arrays->acquire_array<void *>(max))          ||
            !(predicate_lens = arrays->acquire_array<std::size_t>(max))) {
            cleanup();
            return TRANSPORT_SUCCESS;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BDelete::cleanup() {
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
    }

    arrays->release_array(subjects, count);
    subjects = nullptr;

    arrays->release_array(subject_lens, count);
    subject_lens = nullptr;

    arrays->release_array(predicates, count);
    predicates = nullptr;

    arrays->release_array(predicate_lens, count);
    predicate_lens = nullptr;

    Bulk::cleanup(arrays);

    return TRANSPORT_SUCCESS;
}

Transport::Response::BDelete::BDelete(FixedBufferPool *arrays, FixedBufferPool *buffers, const std::size_t max)
    : Response(BDELETE, arrays, buffers),
      Bulk(),
      statuses(nullptr),
      next(nullptr)
{
    alloc(max);
}

Transport::Response::BDelete::~BDelete() {
    cleanup();
}

std::size_t Transport::Response::BDelete::size() const {
    return Response::size() + sizeof(count) + (sizeof(*ds_offsets) * count) + (sizeof(*statuses) * count);
}

int Transport::Response::BDelete::alloc(const std::size_t max) {
    if (max) {
        if ((Bulk::alloc(max, arrays) != TRANSPORT_SUCCESS) ||
            !(statuses = arrays->acquire_array<int>(max)))   {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BDelete::cleanup() {
    arrays->release_array(statuses, count);
    statuses = nullptr;

    Bulk::cleanup(arrays);

    return TRANSPORT_SUCCESS;
}
