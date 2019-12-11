#ifndef TRANSPORT_BGETOP_MESSAGE_HPP
#define TRANSPORT_BGETOP_MESSAGE_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "transport/Messages/Bulk.hpp"
#include "transport/Messages/Request.hpp"
#include "transport/Messages/Response.hpp"
#include "transport/constants.hpp"

namespace Transport {

namespace Request {

struct BGetOp final : Request, Bulk {
    BGetOp(const std::size_t max = 0);
    ~BGetOp();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int cleanup();

    void **subjects;
    std::size_t *subject_lens;
    void **predicates;
    std::size_t *predicate_lens;
    hxhim_type_t *object_types;
    std::size_t *num_recs;            // number of records to get back
    hxhim_get_op_t *ops;
};

}

namespace Response {

struct BGetOp final : Response, Bulk {
    BGetOp(const std::size_t max = 0);
    ~BGetOp();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int merge(BGetOp *bgetop);
    int cleanup();

    int *statuses;

    void **subjects;
    std::size_t *subject_lens;
    void **predicates;
    std::size_t *predicate_lens;
    hxhim_type_t *object_types;
    void **objects;
    std::size_t *object_lens;

    BGetOp *next;
};

}

}

#endif
