#include "transport/Messages/SubjectPredicate.hpp"

Transport::Request::SubjectPredicate::SubjectPredicate(const enum hxhim_op_t op)
    : Request(op),
      subjects(nullptr),
      predicates(nullptr),
      orig()
{}

Transport::Request::SubjectPredicate::~SubjectPredicate() {}

std::size_t Transport::Request::SubjectPredicate::size() const {
    std::size_t total = Request::size();
    for(std::size_t i = 0; i < count; i++) {
        total += subjects[i].pack_size(true) + sizeof(orig.subjects[i]) +
                 predicates[i].pack_size(true) + sizeof(orig.predicates[i]);
    }

    return total;
}

void Transport::Request::SubjectPredicate::alloc(const std::size_t max) {
    if (max) {
        Request::alloc(max);
        subjects        = alloc_array<Blob>(max);
        predicates      = alloc_array<Blob>(max);
        orig.subjects   = alloc_array<void *>(max);
        orig.predicates = alloc_array<void *>(max);
    }
}

int Transport::Request::SubjectPredicate::steal(Transport::Request::SubjectPredicate *from, const std::size_t i) {
    if (Request::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    subjects[count]     = std::move(from->subjects[i]);
    predicates[count]   = std::move(from->predicates[i]);

    orig.subjects[count]     = from->orig.subjects[i];
    orig.predicates[count]   = from->orig.predicates[i];

    from->orig.subjects[i]   = nullptr;
    from->orig.predicates[i] = nullptr;

    return TRANSPORT_SUCCESS;
}

int Transport::Request::SubjectPredicate::cleanup() {
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

Transport::Response::SubjectPredicate::SubjectPredicate(const enum hxhim_op_t op)
    : Response(op),
      orig()
{}

Transport::Response::SubjectPredicate::~SubjectPredicate() {}

std::size_t Transport::Response::SubjectPredicate::size() const {
    std::size_t total = Response::size();
    for(std::size_t i = 0; i < count; i++) {
        total += orig.subjects[i].pack_ref_size(true) +
                 orig.predicates[i].pack_ref_size(true);
    }

    return total;
}

void Transport::Response::SubjectPredicate::alloc(const std::size_t max) {
    if (max) {
        Response::alloc(max);
        orig.subjects   = alloc_array<Blob>(max);
        orig.predicates = alloc_array<Blob>(max);
    }
}

int Transport::Response::SubjectPredicate::steal(Transport::Response::SubjectPredicate *from, const std::size_t i) {
    if (Response::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    orig.subjects[count]     = std::move(from->orig.subjects[i]);
    orig.predicates[count]   = std::move(from->orig.predicates[i]);

    return TRANSPORT_SUCCESS;
}

int Transport::Response::SubjectPredicate::cleanup() {
    dealloc_array(orig.subjects, max_count);
    orig.subjects = nullptr;

    dealloc_array(orig.predicates, max_count);
    orig.predicates = nullptr;

    return Response::cleanup();
}
