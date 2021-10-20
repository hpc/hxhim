#ifndef TRANSPORT_BHISTOGRAM_MESSAGE_HPP
#define TRANSPORT_BHISTOGRAM_MESSAGE_HPP

#include <cstddef>
#include <memory>

#include "hxhim/Blob.hpp"
#include "hxhim/constants.h"
#include "transport/Messages/Request.hpp"
#include "transport/Messages/Response.hpp"
#include "transport/constants.hpp"
#include "utils/Histogram.hpp"

namespace Transport {

namespace Request {

struct BHistogram final : Request {
    BHistogram(const std::size_t max = 0);
    ~BHistogram();

    void alloc(const std::size_t max);
    std::size_t add(Blob name);
    int steal(BHistogram *from, const std::size_t i);
    int cleanup();

    Blob *names;
};

}

namespace Response {

struct BHistogram final : Response {
    BHistogram(const std::size_t max = 0);
    ~BHistogram();

    void alloc(const std::size_t max);
    std::size_t add(const std::shared_ptr<Histogram::Histogram> &hist, int status);
    int steal(BHistogram *from, const std::size_t i);
    int cleanup();

    std::shared_ptr<Histogram::Histogram> *histograms;
};

}

}

#endif
