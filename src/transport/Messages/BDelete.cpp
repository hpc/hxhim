#include "transport/Messages/BDelete.hpp"

Transport::Request::BDelete::BDelete(const std::size_t max)
    : Request(BDELETE),
      subjects(nullptr),
      predicates(nullptr)
{
    alloc(max);
}

Transport::Request::BDelete::~BDelete() {
    cleanup();
}

std::size_t Transport::Request::BDelete::size() const {
    std::size_t total = Request::size();
    for(std::size_t i = 0; i < count; i++) {
        total += subjects[i]->len + sizeof(subjects[i]->len) +
                 predicates[i]->len + sizeof(predicates[i]->len);
    }

    return total;
}

int Transport::Request::BDelete::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Request::alloc(max) != TRANSPORT_SUCCESS) ||
            !(subjects = alloc_array<Blob *>(max))     ||
            !(predicates = alloc_array<Blob *>(max)))   {
            cleanup();
            return TRANSPORT_SUCCESS;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BDelete::steal(Transport::Request::BDelete *from, const std::size_t i) {
    if (Request::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    subjects[count]          = from->subjects[i];
    predicates[count]        = from->predicates[i];

    count++;

    from->subjects[i]        = nullptr;
    from->predicates[i]      = nullptr;

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BDelete::cleanup() {
    for(std::size_t i = 0; i < count; i++) {
        destruct(subjects[i]);
        destruct(predicates[i]);
    }

    dealloc_array(subjects, count);
    subjects = nullptr;

    dealloc_array(predicates, count);
    predicates = nullptr;

    return Request::cleanup();
}

Transport::Response::BDelete::BDelete(const std::size_t max)
    : Response(BDELETE),
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
    cleanup();
    return Response::alloc(max);
}

int Transport::Response::BDelete::steal(Transport::Response::BDelete *bdelete, const std::size_t i) {
    if (Response::steal(bdelete, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    count++;

    return HXHIM_SUCCESS;
}

int Transport::Response::BDelete::cleanup() {
    return Response::cleanup();
}
