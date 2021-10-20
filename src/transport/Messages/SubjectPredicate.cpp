#include "transport/Messages/SubjectPredicate.hpp"

Transport::Request::SubjectPredicate::SubjectPredicate(const enum hxhim_op_t op)
    : Request(op),
      subjects(nullptr),
      predicates(nullptr),
      orig()
{}

Transport::Request::SubjectPredicate::~SubjectPredicate() {}

void Transport::Request::SubjectPredicate::alloc(const std::size_t max) {
    if (max) {
        Request::alloc(max);
        subjects        = alloc_array<Blob>(max);
        predicates      = alloc_array<Blob>(max);
        orig.subjects   = alloc_array<void *>(max);
        orig.predicates = alloc_array<void *>(max);
    }
}

std::size_t Transport::Request::SubjectPredicate::add(Blob subject, Blob predicate, const bool increment_count) {
    subjects[count] = subject;
    predicates[count] = predicate;
    orig.subjects[count] = subject.data();
    orig.predicates[count] = predicate.data();
    return Request::add(subject.pack_size(true) + sizeof(subject.data()) +
                        predicate.pack_size(true) + sizeof(predicate.data()),
                        increment_count);
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

void Transport::Response::SubjectPredicate::alloc(const std::size_t max) {
    if (max) {
        Response::alloc(max);
        orig.subjects   = alloc_array<Blob>(max);
        orig.predicates = alloc_array<Blob>(max);
    }
}

std::size_t Transport::Response::SubjectPredicate::add(Blob &subject, Blob &predicate, int status) {
    orig.subjects[count] = std::move(subject);
    orig.predicates[count] = std::move(predicate);
    return Response::add(status,
                         orig.subjects[count].pack_ref_size(true) +
                         orig.predicates[count].pack_ref_size(true),
                         true);
}

int Transport::Response::SubjectPredicate::steal(Transport::Response::SubjectPredicate *from, const std::size_t i) {
    add(from->orig.subjects[i], from->orig.predicates[i], from->statuses[i]);
    return TRANSPORT_SUCCESS;
}

int Transport::Response::SubjectPredicate::cleanup() {
    dealloc_array(orig.subjects, max_count);
    orig.subjects = nullptr;

    dealloc_array(orig.predicates, max_count);
    orig.predicates = nullptr;

    return Response::cleanup();
}
