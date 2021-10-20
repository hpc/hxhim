#ifndef TRANSPORT_BGETOP_MESSAGE_HPP
#define TRANSPORT_BGETOP_MESSAGE_HPP

#include "transport/Messages/SubjectPredicate.hpp"

namespace Transport {

namespace Request {

struct BGetOp final : SubjectPredicate {
    BGetOp(const std::size_t max = 0);
    ~BGetOp();

    void alloc(const std::size_t max);
    std::size_t add(Blob subject, Blob predicate,
                    hxhim_data_t object_type,
                    std::size_t num_rec,
                    hxhim_getop_t op);
    int steal(BGetOp *from, const std::size_t i);
    int cleanup();

    hxhim_data_t *object_types;
    std::size_t *num_recs;            // number of records to get back
    hxhim_getop_t *ops;
};

}

namespace Response {

struct BGetOp final : Response { // does not inherit SubjectPredicate
    BGetOp(const std::size_t max = 0);
    ~BGetOp();

    void alloc(const std::size_t max);

    // does not add to serialized size
    std::size_t add(Blob *subject,
                    Blob *predicate,
                    Blob *object,
                    std::size_t num_rec,
                    int status);

    // add the size of the latest set of responses
    std::size_t update_size(const std::size_t);

    int steal(BGetOp *bgetop, const std::size_t i);
    int cleanup();

    std::size_t *num_recs;

    // array of array of results
    Blob **subjects;
    Blob **predicates;
    Blob **objects;
};

}

}

#endif
