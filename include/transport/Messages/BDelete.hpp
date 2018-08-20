#ifndef TRANSPORT_BDELETE_MESSAGE_HPP
#define TRANSPORT_BDELETE_MESSAGE_HPP

#include <cstddef>

#include "transport/Messages/Bulk.hpp"
#include "transport/Messages/Request.hpp"
#include "transport/Messages/Response.hpp"
#include "transport/constants.h"

namespace Transport {

namespace Request {

struct BDelete final : Request, Bulk {
    BDelete(FixedBufferPool *fbp, const std::size_t max = 0);
    ~BDelete();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int cleanup();

    void **subjects;
    std::size_t *subject_lens;
    void **predicates;
    std::size_t *predicate_lens;
};

}

namespace Response {

struct BDelete final : Response, Bulk {
    BDelete(FixedBufferPool *fbp, const std::size_t max = 0);
    ~BDelete();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int cleanup();

    int *statuses;

    BDelete *next;
};

}

}

#endif
