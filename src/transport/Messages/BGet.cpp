#include "transport/Messages/BGet.hpp"

Transport::Request::BGet::BGet(const std::size_t max)
    : SubjectPredicate(hxhim_op_t::HXHIM_GET),
      object_types(nullptr)
{
    alloc(max);
}

Transport::Request::BGet::~BGet() {
    cleanup();
}

std::size_t Transport::Request::BGet::size() const {
    std::size_t total = SubjectPredicate::size();
    for(std::size_t i = 0; i < count; i++) {
        total += sizeof(object_types[i]);
    }
    return total;
}

int Transport::Request::BGet::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((SubjectPredicate::alloc(max) != TRANSPORT_SUCCESS)                     ||
            !(object_types                 = alloc_array<hxhim_object_type_t>(max))) {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BGet::steal(Transport::Request::BGet *from, const std::size_t i) {
    if (SubjectPredicate::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    object_types[count] = from->object_types[i];

    count++;

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BGet::cleanup() {
    dealloc_array(object_types, max_count);
    object_types = nullptr;

    return SubjectPredicate::cleanup();
}

Transport::Response::BGet::BGet(const std::size_t max)
    : SubjectPredicate(hxhim_op_t::HXHIM_GET),
      object_types(nullptr),
      objects(nullptr)
{
    alloc(max);
}

Transport::Response::BGet::~BGet() {
    cleanup();
}

std::size_t Transport::Response::BGet::size() const {
    std::size_t total = SubjectPredicate::size();

    for(std::size_t i = 0; i < count; i++) {
        total += sizeof(object_types[i]);

        if (statuses[i] == DATASTORE_SUCCESS) {
            total += objects[i].pack_size();
        }
    }

    return total;
}

int Transport::Response::BGet::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((SubjectPredicate::alloc(max) != TRANSPORT_SUCCESS)             ||
            !(object_types         = alloc_array<hxhim_object_type_t>(max)) ||
            !(objects              = alloc_array<Blob>(max)))                {
            cleanup();
            return TRANSPORT_SUCCESS;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BGet::steal(Transport::Response::BGet *bget, const std::size_t i) {
    if (SubjectPredicate::steal(bget, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    object_types[count]       = bget->object_types[i];
    objects[count]            = bget->objects[i];

    count++;

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BGet::cleanup() {
    dealloc_array(object_types, max_count);
    object_types = nullptr;

    dealloc_array(objects, max_count);
    objects = nullptr;

    return SubjectPredicate::cleanup();
}
