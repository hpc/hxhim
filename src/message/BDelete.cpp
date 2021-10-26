#include "message/BDelete.hpp"

Message::Request::BDelete::BDelete(const std::size_t max)
    : SubjectPredicate(hxhim_op_t::HXHIM_DELETE)
{
    alloc(max);
}

Message::Request::BDelete::~BDelete() {
    cleanup();
}

void Message::Request::BDelete::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        SubjectPredicate::alloc(max);
    }
}

std::size_t Message::Request::BDelete::add(Blob subject, Blob predicate) {
    return SubjectPredicate::add(subject, predicate, true);
}

int Message::Request::BDelete::cleanup() {
    return SubjectPredicate::cleanup();
}

Message::Response::BDelete::BDelete(const std::size_t max)
    : SubjectPredicate(hxhim_op_t::HXHIM_DELETE)
{
    alloc(max);
}

Message::Response::BDelete::~BDelete() {
    cleanup();
}

void Message::Response::BDelete::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        SubjectPredicate::alloc(max);
    }
}

std::size_t Message::Response::BDelete::add(Blob subject,
                                              Blob predicate,
                                              int status) {
    return SubjectPredicate::add(subject, predicate, status);
}

int Message::Response::BDelete::steal(BDelete *from, const std::size_t i) {
    add(std::move(from->orig.subjects[i]),
        std::move(from->orig.predicates[i]),
        from->statuses[i]);
    from->orig.subjects[i] = nullptr;
    from->orig.predicates[i] = nullptr;
    return HXHIM_SUCCESS;
}

int Message::Response::BDelete::cleanup() {
    return SubjectPredicate::cleanup();
}
