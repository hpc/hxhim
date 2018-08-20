#include "transport/Messages/BDelete.hpp"

Transport::Request::BDelete::BDelete(FixedBufferPool *fbp, const std::size_t max)
    : Request(BDELETE, fbp),
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
        if ((Bulk::alloc(max) != TRANSPORT_SUCCESS)    ||
            !(subjects = new void *[max]())            ||
            !(subject_lens = new std::size_t[max]())   ||
            !(predicates = new void *[max]())          ||
            !(predicate_lens = new std::size_t[max]())) {
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

    Bulk::cleanup();

    return TRANSPORT_SUCCESS;
}

Transport::Response::BDelete::BDelete(FixedBufferPool *fbp, const std::size_t max)
    : Response(BDELETE, fbp),
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
            !(statuses = new int[max]()))            {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BDelete::cleanup() {
    delete [] statuses;
    statuses = nullptr;

    Bulk::cleanup();

    return TRANSPORT_SUCCESS;
}
