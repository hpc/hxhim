#ifndef TRANSPORT_GET2_MESSAGE_HPP
#define TRANSPORT_GET2_MESSAGE_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "transport/Messages/Request.hpp"
#include "transport/Messages/Response.hpp"
#include "transport/Messages/Single.hpp"

namespace Transport {

namespace Request {

struct Get2 final : Request, Single {
    Get2();
    ~Get2();

    std::size_t size() const;

    void *subject;
    std::size_t subject_len;
    void *predicate;
    std::size_t predicate_len;
    hxhim_type_t object_type;
    void *object;
    std::size_t *object_len;

    void *src_object;
    size_t *src_object_len;
    std::size_t dst_len;
};

}

namespace Response {

struct Get2 final : Response, Single {
    Get2();
    ~Get2();

    std::size_t size() const;

    int status;

    void *subject;
    std::size_t subject_len;
    void *predicate;
    std::size_t predicate_len;
    hxhim_type_t object_type;
    void *object;
    std::size_t *object_len;

    void *src_object;
    size_t *src_object_len;
};

}

}

#endif
