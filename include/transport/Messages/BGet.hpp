#ifndef TRANSPORT_BGET_MESSAGE_HPP
#define TRANSPORT_BGET_MESSAGE_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "utils/Blob.hpp"
#include "transport/Messages/Request.hpp"
#include "transport/Messages/Response.hpp"
#include "transport/constants.hpp"

namespace Transport {

namespace Request {

struct BGet final : Request {
    BGet(const std::size_t max = 0);
    ~BGet();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int steal(BGet *from, const std::size_t i);
    int cleanup();

    Blob **subjects;
    Blob **predicates;
    hxhim_type_t *object_types;
};

}

namespace Response {

struct BGet final : Response {
    BGet(const std::size_t max = 0);
    ~BGet();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int steal(BGet *bget, const int ds);
    int cleanup();

    Blob **subjects;
    Blob **predicates;
    hxhim_type_t *object_types;
    Blob **objects;

    BGet *next;
};

}

}

#endif
