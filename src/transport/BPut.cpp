#include "transport/BPut.hpp"

Transport::Request::BPut::BPut(const std::size_t max)
    : Request(BPUT),
      Bulk(),
      subjects(nullptr),
      subject_lens(nullptr),
      predicates(nullptr),
      predicate_lens(nullptr),
      object_types(nullptr),
      objects(nullptr),
      object_lens(nullptr)
{
    alloc(max);
}

Transport::Request::BPut::~BPut() {
    cleanup();
}

std::size_t Transport::Request::BPut::size() const {
    std::size_t total = Request::size() + sizeof(count);
    for(std::size_t i = 0; i < count; i++) {
        total += sizeof(ds_offsets[i]) +
            subject_lens[i] + sizeof(subject_lens[i]) +
            predicate_lens[i] + sizeof(predicate_lens[i]) +
            sizeof(object_types[i]) + object_lens[i] + sizeof(object_lens[i]);
    }
    return total;
}

int Transport::Request::BPut::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Bulk::alloc(max) != TRANSPORT_SUCCESS)    ||
            !(subjects = new void *[max]())            ||
            !(subject_lens = new std::size_t[max]())   ||
            !(predicates = new void *[max]())          ||
            !(predicate_lens = new std::size_t[max]()) ||
            !(object_types = new hxhim_type_t[max]())  ||
            !(objects = new void *[max]())             ||
            !(object_lens = new std::size_t[max]()))    {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BPut::cleanup() {
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

    Bulk::cleanup();

    return TRANSPORT_SUCCESS;
}

Transport::Response::BPut::BPut(const std::size_t max)
    : Response(Message::BPUT),
      Bulk(),
      statuses(nullptr),
      next(nullptr)
{
    alloc(max);
}

Transport::Response::BPut::~BPut() {
    cleanup();
}

std::size_t Transport::Response::BPut::size() const {
    return Response::size() + sizeof(count) + (sizeof(*ds_offsets) * count) + (sizeof(*statuses) * count);
}

int Transport::Response::BPut::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Bulk::alloc(max) != TRANSPORT_SUCCESS) ||
            !(statuses = new int[max]))              {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BPut::cleanup() {
    delete [] statuses;
    statuses = nullptr;

    Bulk::cleanup();

    return TRANSPORT_SUCCESS;
}
