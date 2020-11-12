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

std::size_t Transport::Request::BPut::size() const {
    std::size_t total = SubjectPredicate::size();
    for(std::size_t i = 0; i < count; i++) {
        total += objects[i].pack_size(true);
    }
    return total;
}

void Transport::Request::BPut::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        SubjectPredicate::alloc(max);
        objects = alloc_array<Blob>(max);
    }
}

int Transport::Request::BPut::steal(Transport::Request::BPut *from, const std::size_t i) {
    if (SubjectPredicate::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    objects[count] = from->objects[i];

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

std::size_t Transport::Response::BPut::size() const {
    return SubjectPredicate::size();
}

void Transport::Response::BPut::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        SubjectPredicate::alloc(max);
    }
}

int Transport::Response::BPut::steal(Transport::Response::BPut *from, const std::size_t i) {
    if (SubjectPredicate::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    count++;

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BPut::cleanup() {
    return SubjectPredicate::cleanup();
}
