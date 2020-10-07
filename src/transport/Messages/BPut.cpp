#include "transport/Messages/BPut.hpp"

Transport::Request::BPut::BPut(const std::size_t max)
    : SubjectPredicate(hxhim_op_t::HXHIM_PUT),
      object_types(nullptr),
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
        total += sizeof(object_types[i]) +
                 objects[i].pack_size();
    }
    return total;
}

void Transport::Request::BPut::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        SubjectPredicate::alloc(max);
        object_types = alloc_array<hxhim_object_type_t>(max);
        objects      = alloc_array<Blob>(max);
    }
}

int Transport::Request::BPut::steal(Transport::Request::BPut *from, const std::size_t i) {
    if (SubjectPredicate::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    object_types[count] = from->object_types[i];
    objects[count]      = from->objects[i];

    count++;

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BPut::cleanup() {
    dealloc_array(object_types, max_count);
    object_types = nullptr;

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
