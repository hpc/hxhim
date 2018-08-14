#ifndef TRANSPORT_HISTOGRAM_MESSAGE_HPP
#define TRANSPORT_HISTOGRAM_MESSAGE_HPP

#include <cstddef>
#include <map>

#include "hxhim/constants.h"
#include "transport/Request.hpp"
#include "transport/Response.hpp"
#include "transport/Single.hpp"
#include "transport/constants.h"

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
    Histogram(const std::map<double, std::size_t> &hist = {});
    ~Histogram();

    std::size_t size() const;

    int status;

    std::map<double, std::size_t> hist;
};

}

}

#endif
