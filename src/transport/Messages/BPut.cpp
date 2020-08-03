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

int Transport::Request::BPut::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((SubjectPredicate::alloc(max) != TRANSPORT_SUCCESS)                     ||
            !(object_types                 = alloc_array<hxhim_object_type_t>(max)) ||
            !(objects                      = alloc_array<Blob>(max)))                {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
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
    std::size_t total = SubjectPredicate::size();
    return total;
}

int Transport::Response::BPut::alloc(const std::size_t max) {
    cleanup();
    if (max) {
        if ((SubjectPredicate::alloc(max) != TRANSPORT_SUCCESS)       ||
            !(orig.subjects                = alloc_array<Blob>(max))  ||
            !(orig.predicates              = alloc_array<Blob>(max)))  {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BPut::steal(Transport::Response::BPut *from, const std::size_t i) {
    if (SubjectPredicate::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    orig.subjects[count]   = from->orig.subjects[i];
    orig.predicates[count] = from->orig.predicates[i];

    count++;

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BPut::cleanup() {
    dealloc_array(orig.subjects, max_count);
    orig.subjects = nullptr;

    dealloc_array(orig.predicates, max_count);
    orig.predicates = nullptr;

    return SubjectPredicate::cleanup();
}
