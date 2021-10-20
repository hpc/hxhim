#include "transport/Messages/BDelete.hpp"

Transport::Request::BDelete::BDelete(const std::size_t max)
    : SubjectPredicate(hxhim_op_t::HXHIM_DELETE)
{
    alloc(max);
}

Transport::Request::BDelete::~BDelete() {
    cleanup();
}

void Transport::Request::BDelete::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        SubjectPredicate::alloc(max);
    }
}

std::size_t Transport::Request::BDelete::add(Blob subject, Blob predicate) {
    return SubjectPredicate::add(subject, predicate, true);
}

int Transport::Request::BDelete::steal(Transport::Request::BDelete *from, const std::size_t i) {
    if (SubjectPredicate::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    count++;

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BDelete::cleanup() {
    return SubjectPredicate::cleanup();
}

Transport::Response::BDelete::BDelete(const std::size_t max)
    : SubjectPredicate(hxhim_op_t::HXHIM_DELETE)
{
    alloc(max);
}

Transport::Response::BDelete::~BDelete() {
    cleanup();
}

void Transport::Response::BDelete::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        SubjectPredicate::alloc(max);
    }
}

std::size_t Transport::Response::BDelete::add(Blob subject,
                                              Blob predicate,
                                              int status) {
    return SubjectPredicate::add(subject, predicate, status);
}

int Transport::Response::BDelete::steal(Transport::Response::BDelete *from, const std::size_t i) {
    add(std::move(from->orig.subjects[i]),
        std::move(from->orig.predicates[i]),
        from->statuses[i]);
    from->orig.subjects[i] = nullptr;
    from->orig.predicates[i] = nullptr;
    return HXHIM_SUCCESS;
}

int Transport::Response::BDelete::cleanup() {
    return SubjectPredicate::cleanup();
}
