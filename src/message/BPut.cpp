#include "message/BPut.hpp"

Message::Request::BPut::BPut(const std::size_t max)
    : SubjectPredicate(hxhim_op_t::HXHIM_PUT),
      objects(nullptr)
{
    alloc(max);
}

Message::Request::BPut::~BPut() {
    cleanup();
}

void Message::Request::BPut::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        SubjectPredicate::alloc(max);
        objects = alloc_array<Blob>(max);
    }
}

std::size_t Message::Request::BPut::add(Blob subject, Blob predicate, Blob object) {
    objects[count] = object;
    Request::add(object.pack_size(true), false);
    return SubjectPredicate::add(subject, predicate, true);
}

int Message::Request::BPut::cleanup() {
    dealloc_array(objects, max_count);
    objects = nullptr;

    return SubjectPredicate::cleanup();
}

Message::Response::BPut::BPut(const std::size_t max)
    : SubjectPredicate(hxhim_op_t::HXHIM_PUT)
{
    alloc(max);
}

Message::Response::BPut::~BPut() {
    cleanup();
}

void Message::Response::BPut::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        SubjectPredicate::alloc(max);
    }
}

std::size_t Message::Response::BPut::add(Blob subject, Blob predicate, int status) {
    return SubjectPredicate::add(subject, predicate, status);
}

int Message::Response::BPut::steal(BPut *from, const std::size_t i) {
    add(std::move(from->orig.subjects[i]),
        std::move(from->orig.predicates[i]),
        from->statuses[i]);
    from->orig.subjects[i] = nullptr;
    from->orig.predicates[i] = nullptr;
    return MESSAGE_SUCCESS;
}

int Message::Response::BPut::cleanup() {
    return SubjectPredicate::cleanup();
}
