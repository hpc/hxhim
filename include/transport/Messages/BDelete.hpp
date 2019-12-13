#ifndef TRANSPORT_BDELETE_MESSAGE_HPP
#define TRANSPORT_BDELETE_MESSAGE_HPP

#include <cstddef>

#include "transport/Messages/Request.hpp"
#include "transport/Messages/Response.hpp"
#include "transport/constants.hpp"

namespace Transport {

namespace Request {

struct BDelete final : Request {
    BDelete(const std::size_t max = 0);
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

struct BDelete final : Response {
    BDelete(const std::size_t max = 0);
    ~BDelete();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int merge(BDelete *bdel);
    int cleanup();

    int *statuses;

    BDelete *next;
};

}

}

#endif
