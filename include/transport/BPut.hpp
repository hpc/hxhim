#ifndef TRANSPORT_BPUT_MESSAGE_HPP
#define TRANSPORT_BPUT_MESSAGE_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "transport/Bulk.hpp"
#include "transport/Request.hpp"
#include "transport/Response.hpp"
#include "transport/constants.h"

namespace Transport {

namespace Request {

struct BPut final : Request, Bulk {
    BPut(const std::size_t max = 0);
    ~BPut();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int cleanup();

    void **subjects;
    std::size_t *subject_lens;
    void **predicates;
    std::size_t *predicate_lens;
    hxhim_type_t *object_types;
    void **objects;
    std::size_t *object_lens;
};

}

namespace Response {

struct BPut final : Response, Bulk {
    BPut(const std::size_t max = 0);
    ~BPut();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int cleanup();

    int *statuses;

    BPut *next;
};

}

}

#endif
