#include "transport/Messages/BDelete.hpp"

Transport::Request::BDelete::BDelete(const std::size_t max)
    : Request(hxhim_op_t::HXHIM_DELETE),
      subjects(nullptr),
      predicates(nullptr),
      orig()
{
    alloc(max);
}

Transport::Request::BDelete::~BDelete() {
    cleanup();
}

std::size_t Transport::Request::BDelete::size() const {
    std::size_t total = Request::size();
    for(std::size_t i = 0; i < count; i++) {
        total += subjects[i].pack_size() + sizeof(orig.subjects[i]) +
                 predicates[i].pack_size() + sizeof(orig.predicates[i]);
    }

    return total;
}

int Transport::Request::BDelete::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Request::alloc(max) != TRANSPORT_SUCCESS)        ||
            !(subjects            = alloc_array<Blob>(max))   ||
            !(predicates          = alloc_array<Blob>(max))   ||
            !(orig.subjects       = alloc_array<void *>(max)) ||
            !(orig.predicates     = alloc_array<void *>(max))) {
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

    subjects[count]     = from->subjects[i];
    predicates[count]   = from->predicates[i];

    orig.subjects[count]     = from->orig.subjects[i];
    orig.predicates[count]   = from->orig.predicates[i];

    count++;

    from->orig.subjects[i]   = nullptr;
    from->orig.predicates[i] = nullptr;

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BDelete::cleanup() {
    dealloc_array(subjects, max_count);
    subjects = nullptr;

    dealloc_array(predicates, max_count);
    predicates = nullptr;

    dealloc_array(orig.subjects, max_count);
    orig.subjects = nullptr;

    dealloc_array(orig.predicates, max_count);
    orig.predicates = nullptr;

    return Request::cleanup();
}

Transport::Response::BDelete::BDelete(const std::size_t max)
    : Response(hxhim_op_t::HXHIM_DELETE),
      orig()
{
    alloc(max);
}

Transport::Response::BDelete::~BDelete() {
    cleanup();
}

std::size_t Transport::Response::BDelete::size() const {
    std::size_t total = Response::size();
    for(std::size_t i = 0; i < count; i++) {
        total += orig.subjects[i].pack_ref_size() +
                 orig.predicates[i].pack_ref_size();
    }

    return total;
}

int Transport::Response::BDelete::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Response::alloc(max) != TRANSPORT_SUCCESS)               ||
            !(orig.subjects        = alloc_array<ReferenceBlob>(max)) ||
            !(orig.predicates      = alloc_array<ReferenceBlob>(max))) {
            cleanup();
            return TRANSPORT_SUCCESS;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BDelete::steal(Transport::Response::BDelete *from, const std::size_t i) {
    if (Response::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    orig.subjects[count]     = from->orig.subjects[i];
    orig.predicates[count]   = from->orig.predicates[i];

    count++;

    return HXHIM_SUCCESS;
}

int Transport::Response::BDelete::cleanup() {
    dealloc_array(orig.subjects, max_count);
    orig.subjects = nullptr;

    dealloc_array(orig.predicates, max_count);
    orig.predicates = nullptr;

    return Response::cleanup();
}
