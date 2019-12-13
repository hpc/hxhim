#include "transport/Messages/BDelete.hpp"

Transport::Request::BDelete::BDelete(const std::size_t max)
    : Request(BDELETE),
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
        if ((Bulk::alloc(max) != TRANSPORT_SUCCESS)           ||
            !(subjects = alloc_array<void *>(max))            ||
            !(subject_lens = alloc_array<std::size_t>(max))   ||
            !(predicates = alloc_array<void *>(max))          ||
            !(predicate_lens = alloc_array<std::size_t>(max))) {
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

    return TRANSPORT_SUCCESS;
}

Transport::Response::BDelete::BDelete(const std::size_t max)
    : Response(BDELETE),
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
        if ((Bulk::alloc(max) != TRANSPORT_SUCCESS) ||
            !(statuses = alloc_array<int>(max)))     {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BDelete::cleanup() {
    dealloc_array(statuses, count);
    statuses = nullptr;

    return TRANSPORT_SUCCESS;
}
