#include "message/BGet.hpp"

Message::Request::BGet::BGet(const std::size_t max)
    : SubjectPredicate(hxhim_op_t::HXHIM_GET),
      object_types(nullptr)
{
    alloc(max);
}

Message::Request::BGet::~BGet() {
    cleanup();
}

void Message::Request::BGet::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        SubjectPredicate::alloc(max);
        object_types = alloc_array<hxhim_data_t>(max);
    }
}

std::size_t Message::Request::BGet::add(Blob subject, Blob predicate, hxhim_data_t object_type) {
    object_types[count] = object_type;
    Request::add(sizeof(object_type), false);
    return SubjectPredicate::add(subject, predicate, true);
}

int Message::Request::BGet::cleanup() {
    dealloc_array(object_types, max_count);
    object_types = nullptr;

    return SubjectPredicate::cleanup();
}

Message::Response::BGet::BGet(const std::size_t max)
    : SubjectPredicate(hxhim_op_t::HXHIM_GET),
      objects(nullptr)
{
    alloc(max);
}

Message::Response::BGet::~BGet() {
    cleanup();
}

void Message::Response::BGet::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        SubjectPredicate::alloc(max);
        objects = alloc_array<Blob>(max);
    }
}

std::size_t Message::Response::BGet::add(Blob subject, Blob predicate,
                                           Blob &&object, int status) {
    if (status == DATASTORE_SUCCESS) {
        objects[count] = std::move(object);
        Message::add(objects[count].pack_size(true), false);
    }

    return SubjectPredicate::add(subject, predicate, status);
}

int Message::Response::BGet::steal(BGet *from, const std::size_t i) {
    add((from->orig.subjects[i]),
        (from->orig.predicates[i]),
        std::move(from->objects[i]),
        from->statuses[i]);
    from->orig.subjects[i] = nullptr;
    from->orig.predicates[i] = nullptr;
    from->objects[i] = nullptr;
    return MESSAGE_SUCCESS;
}

int Message::Response::BGet::cleanup() {
    dealloc_array(objects, max_count);
    objects = nullptr;

    return SubjectPredicate::cleanup();
}
