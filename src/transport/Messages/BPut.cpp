#include "transport/Messages/BPut.hpp"

Transport::Request::BPut::BPut(const std::size_t max)
    : SubjectPredicate(hxhim_op_t::HXHIM_PUT),
      objects(nullptr)
{
    alloc(max);
}

Transport::Request::BPut::~BPut() {
    cleanup();
}

void Transport::Request::BPut::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        SubjectPredicate::alloc(max);
        objects = alloc_array<Blob>(max);
    }
}

std::size_t Transport::Request::BPut::add(Blob subject, Blob predicate, Blob object) {
    objects[count] = object;
    Request::add(object.pack_size(true), false);
    return SubjectPredicate::add(subject, predicate, true);
}

int Transport::Request::BPut::steal(Transport::Request::BPut *from, const std::size_t i) {
    if (SubjectPredicate::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    objects[count] = std::move(from->objects[i]);

    count++;

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BPut::cleanup() {
    dealloc_array(objects, max_count);
    objects = nullptr;

    return SubjectPredicate::cleanup();
}

Transport::Response::BPut::BPut(const std::size_t max)
    : SubjectPredicate(hxhim_op_t::HXHIM_PUT)
{
    alloc(max);
}

Transport::Response::BPut::~BPut() {
    cleanup();
}

void Transport::Response::BPut::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        SubjectPredicate::alloc(max);
    }
}

std::size_t Transport::Response::BPut::add(Blob subject, Blob predicate, int status) {
    return SubjectPredicate::add(subject, predicate, status);
}

int Transport::Response::BPut::steal(Transport::Response::BPut *from, const std::size_t i) {
    add(std::move(from->orig.subjects[i]),
        std::move(from->orig.predicates[i]),
        from->statuses[i]);
    from->orig.subjects[i] = nullptr;
    from->orig.predicates[i] = nullptr;
    return TRANSPORT_SUCCESS;
}

int Transport::Response::BPut::cleanup() {
    return SubjectPredicate::cleanup();
}
