#ifndef TRANSPORT_BGET2_MESSAGE_HPP
#define TRANSPORT_BGET2_MESSAGE_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "transport/Messages/Bulk.hpp"
#include "transport/Messages/Request.hpp"
#include "transport/Messages/Response.hpp"
#include "transport/constants.hpp"

namespace Transport {

namespace Request {

struct BGet2 final : Request, Bulk {
    BGet2(const std::size_t max = 0);
    ~BGet2();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int cleanup();

    void **subjects;
    std::size_t *subject_lens;
    void **predicates;
    std::size_t *predicate_lens;
    hxhim_type_t *object_types;
    void **objects;
    std::size_t **object_lens;

    void **src_objects;
    size_t **src_object_lens;
};

}

namespace Response {

struct BGet2 final : Response, Bulk {
    BGet2(const std::size_t max = 0);
    ~BGet2();

    void server_side_cleanup(void * args = nullptr);

    std::size_t size() const;

    int alloc(const std::size_t max);
    int cleanup();

    int *statuses;

    void **subjects;
    std::size_t *subject_lens;
    void **predicates;
    std::size_t *predicate_lens;
    hxhim_type_t *object_types;
    void **objects;
    std::size_t **object_lens;

    void **src_objects;
    size_t **src_object_lens;

    BGet2 *next;
};

}

}

#endif
