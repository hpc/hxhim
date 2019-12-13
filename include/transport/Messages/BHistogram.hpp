#ifndef TRANSPORT_BHISTOGRAM_MESSAGE_HPP
#define TRANSPORT_BHISTOGRAM_MESSAGE_HPP

#include <cstddef>
#include <map>

#include "hxhim/constants.h"
#include "transport/Messages/Bulk.hpp"
#include "transport/Messages/Request.hpp"
#include "transport/Messages/Response.hpp"
#include "transport/constants.hpp"

namespace Transport {

namespace Request {

struct BHistogram final : Request, Bulk {
    BHistogram(const std::size_t max = 0);
    ~BHistogram();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int cleanup();
};

}

namespace Response {

struct BHistogram final : Response, Bulk {
    BHistogram(const std::size_t max = 0);
    ~BHistogram();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int merge(BHistogram *bhist);
    int cleanup();

    int *statuses;

    struct Histogram {
        double *buckets;
        std::size_t *counts;
        std::size_t size;
    };

    Histogram *hists;

    BHistogram *next;
};

}

}

#endif
