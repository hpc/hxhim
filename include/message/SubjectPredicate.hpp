#ifndef SUBJECTPREDICATE_MESSAGE_HPP
#define SUBJECTPREDICATE_MESSAGE_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "message/Request.hpp"
#include "message/Response.hpp"
#include "utils/Blob.hpp"

namespace Message {

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
