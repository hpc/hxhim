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

void Transport::Request::BGet::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        SubjectPredicate::alloc(max);
        object_types = alloc_array<hxhim_data_t>(max);
    }
}

int Transport::Request::BGet::steal(Transport::Request::BGet *from, const std::size_t i) {
    if (SubjectPredicate::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    object_types[count] = std::move(from->object_types[i]);

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
        if (statuses[i] == DATASTORE_SUCCESS) {
            total += objects[i].pack_size(true);
        }
    }

    return total;
}

void Transport::Response::BGet::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        SubjectPredicate::alloc(max);
        objects = alloc_array<Blob>(max);
    }
}

int Transport::Response::BGet::steal(Transport::Response::BGet *bget, const std::size_t i) {
    if (SubjectPredicate::steal(bget, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    objects[count] = std::move(bget->objects[i]);

    count++;

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BGet::cleanup() {
    dealloc_array(objects, max_count);
    objects = nullptr;

    return SubjectPredicate::cleanup();
}
