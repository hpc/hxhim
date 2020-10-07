#ifndef TRANSPORT_BSUBJECTPREDICATE_MESSAGE_HPP
#define TRANSPORT_BSUBJECTPREDICATE_MESSAGE_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "utils/Blob.hpp"
#include "transport/Messages/Request.hpp"
#include "transport/Messages/Response.hpp"
#include "transport/constants.hpp"

namespace Transport {

namespace Request {

struct SubjectPredicate : Request {
    SubjectPredicate(const enum hxhim_op_t op);
    virtual ~SubjectPredicate();

    virtual std::size_t size() const;

    virtual void alloc(const std::size_t max);
    virtual int steal(SubjectPredicate *from, const std::size_t i);
    virtual int cleanup();

    Blob *subjects;
    Blob *predicates;

    // only the pointer value matters, not the data being pointed to
    struct {
        void **subjects;
        void **predicates;
    } orig;
};

}

namespace Response {

struct SubjectPredicate : Response {
    SubjectPredicate(const enum hxhim_op_t op);
    virtual ~SubjectPredicate();

    virtual std::size_t size() const;

    virtual void alloc(const std::size_t max);
    virtual int steal(SubjectPredicate *from, const std::size_t i);
    virtual int cleanup();

    // only the pointer value matters, not the data being pointed to
    struct {
        Blob *subjects;
        Blob *predicates;
    } orig;
};

}

}

#endif
