#ifndef TRANSPORT_GET_MESSAGE_HPP
#define TRANSPORT_GET_MESSAGE_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "transport/Bulk.hpp"
#include "transport/Request.hpp"
#include "transport/Response.hpp"
#include "transport/Single.hpp"

namespace Transport {

namespace Request {

struct Get final : Request, Single {
    Get();
    ~Get();

    std::size_t size() const;

    void *subject;
    std::size_t subject_len;
    void *predicate;
    std::size_t predicate_len;
    hxhim_type_t object_type;
};

}

namespace Response {

struct Get final : Response, Single {
    Get();
    ~Get();

    std::size_t size() const;

    int status;

    void *subject;
    std::size_t subject_len;
    void *predicate;
    std::size_t predicate_len;
    hxhim_type_t object_type;
    void *object;
    std::size_t object_len;
};

}

}

#endif
