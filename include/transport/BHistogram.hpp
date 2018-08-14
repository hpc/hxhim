#ifndef TRANSPORT_BHISTOGRAM_MESSAGE_HPP
#define TRANSPORT_BHISTOGRAM_MESSAGE_HPP

#include <cstddef>
#include <map>

#include "hxhim/constants.h"
#include "transport/Bulk.hpp"
#include "transport/Request.hpp"
#include "transport/Response.hpp"
#include "transport/constants.h"

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
    int cleanup();

    int *statuses;

    std::map<double, std::size_t> *hists;

    BHistogram *next;
};

}

}

#endif
