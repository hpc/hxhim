#ifndef TRANSPORT_BGETOP_MESSAGE_HPP
#define TRANSPORT_BGETOP_MESSAGE_HPP

#include "transport/Messages/SubjectPredicate.hpp"

namespace Transport {

namespace Request {

struct BGetOp final : SubjectPredicate {
    BGetOp(const std::size_t max = 0);
    ~BGetOp();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int steal(BGetOp *from, const std::size_t i);
    int cleanup();

    hxhim_object_type_t *object_types;
    std::size_t *num_recs;            // number of records to get back
    hxhim_get_op_t *ops;
};

}

namespace Response {

struct BGetOp final : Response { // does not inherit SubjectPredicate
    BGetOp(const std::size_t max = 0);
    ~BGetOp();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int steal(BGetOp *bgetop, const std::size_t i);
    int cleanup();

    hxhim_object_type_t *object_types;
    std::size_t *num_recs;

    // array of array of results
    Blob **subjects;
    Blob **predicates;
    Blob **objects;

    BGetOp *next;
};

}

}

#endif
