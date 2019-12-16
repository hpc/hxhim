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
    int merge(BGet *bget);
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
