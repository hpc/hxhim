#ifndef TRANSPORT_HISTOGRAM_MESSAGE_HPP
#define TRANSPORT_HISTOGRAM_MESSAGE_HPP

#include <cstddef>
#include <map>

#include "hxhim/constants.h"
#include "transport/Messages/Request.hpp"
#include "transport/Messages/Response.hpp"
#include "transport/Messages/Single.hpp"
#include "transport/constants.hpp"
#include "utils/Histogram.hpp"

namespace Transport {

namespace Request {

struct Histogram final : Request, Single {
    Histogram();
    ~Histogram();

    std::size_t size() const;
};

}

namespace Response {

struct Histogram final : Response, Single {
    Histogram();
    ~Histogram();

    std::size_t size() const;

    int status;

    struct {
        double *buckets;
        std::size_t *counts;
        std::size_t size;
    } hist;
};

}

}

#endif
