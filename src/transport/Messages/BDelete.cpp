#include "transport/Messages/BDelete.hpp"

Transport::Request::BDelete::BDelete(const std::size_t max)
    : SubjectPredicate(hxhim_op_t::HXHIM_DELETE)
{
    alloc(max);
}

Transport::Request::BDelete::~BDelete() {
    cleanup();
}

std::size_t Transport::Request::BDelete::size() const {
    return SubjectPredicate::size();
}

int Transport::Request::BDelete::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if (SubjectPredicate::alloc(max) != TRANSPORT_SUCCESS) {
            cleanup();
            return TRANSPORT_SUCCESS;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BDelete::steal(Transport::Request::BDelete *from, const std::size_t i) {
    if (SubjectPredicate::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    count++;

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BDelete::cleanup() {
    return SubjectPredicate::cleanup();
}

Transport::Response::BDelete::BDelete(const std::size_t max)
    : SubjectPredicate(hxhim_op_t::HXHIM_DELETE)
{
    alloc(max);
}

Transport::Response::BDelete::~BDelete() {
    cleanup();
}

std::size_t Transport::Response::BDelete::size() const {
    return SubjectPredicate::size();
}

int Transport::Response::BDelete::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if (SubjectPredicate::alloc(max) != TRANSPORT_SUCCESS) {
            cleanup();
            return TRANSPORT_SUCCESS;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BDelete::steal(Transport::Response::BDelete *from, const std::size_t i) {
    if (SubjectPredicate::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    count++;

    return HXHIM_SUCCESS;
}

int Transport::Response::BDelete::cleanup() {
    return SubjectPredicate::cleanup();
}
