#ifndef TRANSPORT_BSUBJECTPREDICATE_MESSAGE_HPP
#define TRANSPORT_BSUBJECTPREDICATE_MESSAGE_HPP

#include <cstddef>

#include "hxhim/Blob.hpp"
#include "hxhim/constants.h"
#include "transport/Messages/Request.hpp"
#include "transport/Messages/Response.hpp"
#include "transport/constants.hpp"

namespace Transport {

namespace Request {

struct SubjectPredicate : Request {
    SubjectPredicate(const enum hxhim_op_t op);
    virtual ~SubjectPredicate();

    virtual void alloc(const std::size_t max);
    virtual std::size_t add(Blob subject, Blob predicate, const bool increment_count);
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

    virtual void alloc(const std::size_t max);
    virtual std::size_t add(Blob &subject, Blob &predicate, int status);
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
