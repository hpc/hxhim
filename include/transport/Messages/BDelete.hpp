#ifndef TRANSPORT_BDELETE_MESSAGE_HPP
#define TRANSPORT_BDELETE_MESSAGE_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "utils/Blob.hpp"
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
    int steal(BDelete *from, const std::size_t i);
    int cleanup();

    Blob *subjects;
    Blob *predicates;

    struct {
        void **subjects;
        void **predicates;
    } orig;
};

}

namespace Response {

struct BDelete final : Response {
    BDelete(const std::size_t max = 0);
    ~BDelete();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int steal(BDelete *from, const std::size_t i);
    int cleanup();

    struct {
        Blob *subjects;
        Blob *predicates;
    } orig;

    BDelete *next;
};

}

}

#endif
