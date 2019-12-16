#ifndef TRANSPORT_BGETOP_MESSAGE_HPP
#define TRANSPORT_BGETOP_MESSAGE_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "utils/Blob.hpp"
#include "transport/Messages/Request.hpp"
#include "transport/Messages/Response.hpp"
#include "transport/constants.hpp"

namespace Transport {

namespace Request {

struct BGetOp final : Request {
    BGetOp(const std::size_t max = 0);
    ~BGetOp();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int cleanup();

    Blob **subjects;
    Blob **predicates;
    hxhim_type_t *object_types;
    std::size_t *num_recs;            // number of records to get back
    hxhim_get_op_t *ops;
};

}

namespace Response {

struct BGetOp final : Response {
    BGetOp(const std::size_t max = 0);
    ~BGetOp();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int merge(BGetOp *bgetop);
    int cleanup();

    Blob **subjects;
    Blob **predicates;
    hxhim_type_t *object_types;
    Blob **objects;

    BGetOp *next;
};

}

}

#endif
