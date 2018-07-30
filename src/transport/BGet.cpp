#include "transport/BGet.hpp"

Transport::Request::BGet::BGet(const std::size_t max)
    : Request(BGET),
      Bulk(),
      subjects(nullptr),
      subject_lens(nullptr),
      predicates(nullptr),
      predicate_lens(nullptr),
      object_types(nullptr)
{
    alloc(max);
}

Transport::Request::BGet::~BGet() {
    cleanup();
}

std::size_t Transport::Request::BGet::size() const {
    std::size_t total = Request::size() + sizeof(count);
    for(std::size_t i = 0; i < count; i++) {
        total += sizeof(db_offsets[i]) +
            subject_lens[i] + sizeof(subject_lens[i]) +
            predicate_lens[i] + sizeof(predicate_lens[i]) +
            sizeof(object_types[i]);
    }
    return total;
}

int Transport::Request::BGet::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Bulk::alloc(max) != TRANSPORT_SUCCESS)  ||
            !(subjects = new void *[max])            ||
            !(subject_lens = new std::size_t[max])   ||
            !(predicates = new void *[max])          ||
            !(predicate_lens = new std::size_t[max]) ||
            !(object_types = new hxhim_type_t[max]))  {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BGet::cleanup() {
    if (clean) {
        if (subjects) {
            for(std::size_t i = 0; i < count; i++) {
                ::operator delete(subjects[i]);
            }
        }

        if (predicates) {
            for(std::size_t i = 0; i < count; i++) {
                ::operator delete(predicates[i]);
            }
        }
    }

    delete [] subjects;
    subjects = nullptr;

    delete [] subject_lens;
    subject_lens = nullptr;

    delete [] predicates;
    predicates = nullptr;

    delete [] predicate_lens;
    predicate_lens = nullptr;

    delete [] object_types;
    object_types = nullptr;

    Bulk::cleanup();

    return TRANSPORT_SUCCESS;
}

Transport::Response::BGet::BGet(const std::size_t max)
    : Response(BGET),
      Bulk(),
      statuses(nullptr),
      subjects(nullptr),
      subject_lens(nullptr),
      predicates(nullptr),
      predicate_lens(nullptr),
      object_types(nullptr),
      objects(nullptr),
      object_lens(nullptr),
      next(nullptr)
{
    alloc(max);
}

Transport::Response::BGet::~BGet() {
    cleanup();
}

std::size_t Transport::Response::BGet::size() const {
    std::size_t total = Response::size() + sizeof(count);
    for(std::size_t i = 0; i < count; i++) {
        total += sizeof(db_offsets[i]) +
            sizeof(statuses[i]) +
            subject_lens[i] + sizeof(subject_lens[i]) +
            predicate_lens[i] + sizeof(predicate_lens[i]) +
            sizeof(object_types[i]) + object_lens[i] + sizeof(object_lens[i]);
    }
    return total;
}

int Transport::Response::BGet::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Bulk::alloc(max) != TRANSPORT_SUCCESS)  ||
            !(statuses = new int[max])               ||
            !(subjects = new void *[max])            ||
            !(subject_lens = new std::size_t[max])   ||
            !(predicates = new void *[max])          ||
            !(predicate_lens = new std::size_t[max]) ||
            !(object_types = new hxhim_type_t[max])  ||
            !(objects = new void *[max])             ||
            !(object_lens = new std::size_t[max]))    {
            cleanup();
            return TRANSPORT_SUCCESS;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BGet::cleanup() {
    if (clean) {
        if (subjects) {
            for(std::size_t i = 0; i < count; i++) {
                ::operator delete(subjects[i]);
            }
        }

        if (predicates) {
            for(std::size_t i = 0; i < count; i++) {
                ::operator delete(predicates[i]);
            }
        }

        if (objects) {
            for(std::size_t i = 0; i < count; i++) {
                ::operator delete(objects[i]);
            }
        }
    }

    delete [] statuses;
    statuses = nullptr;

    delete [] subjects;
    subjects = nullptr;

    delete [] subject_lens;
    subject_lens = nullptr;

    delete [] predicates;
    predicates = nullptr;

    delete [] predicate_lens;
    predicate_lens = nullptr;

    delete [] object_types;
    object_types = nullptr;

    delete [] objects;
    objects = nullptr;

    delete [] object_lens;
    object_lens = nullptr;

    delete next;
    next = nullptr;

    Bulk::cleanup();

    return TRANSPORT_SUCCESS;
}
