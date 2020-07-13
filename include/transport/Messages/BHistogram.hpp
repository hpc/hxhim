#ifndef TRANSPORT_BHISTOGRAM_MESSAGE_HPP
#define TRANSPORT_BHISTOGRAM_MESSAGE_HPP

#include <cstddef>
#include <map>

#include "hxhim/constants.h"
#include "utils/Blob.hpp"
#include "transport/Messages/Request.hpp"
#include "transport/Messages/Response.hpp"
#include "transport/constants.hpp"

namespace Transport {

namespace Request {

struct BHistogram final : Request {
    BHistogram(const std::size_t max = 0);
    ~BHistogram();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int steal(BHistogram *from, const std::size_t i);
    int cleanup();
};

}

namespace Response {

struct BHistogram final : Response {
    BHistogram(const std::size_t max = 0);
    ~BHistogram();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int steal(BHistogram *bhist, std::size_t i);
    int cleanup();

    RealBlob **hists;

    BHistogram *next;
};

}

}

#endif
