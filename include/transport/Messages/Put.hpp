#ifndef TRANSPORT_PUT_MESSAGE_HPP
#define TRANSPORT_PUT_MESSAGE_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "transport/Messages/Request.hpp"
#include "transport/Messages/Response.hpp"
#include "transport/Messages/Single.hpp"
#include "transport/constants.hpp"

namespace Transport {

namespace Request {

struct Put final : Request, Single {
    Put(FixedBufferPool *arrays, FixedBufferPool *buffers);
    ~Put();

    std::size_t size() const;

    void *subject;
    std::size_t subject_len;
    void *predicate;
    std::size_t predicate_len;
    hxhim_type_t object_type;
    void *object;
    std::size_t object_len;
};

}

namespace Response {

struct Put final : Response, Single {
    Put(FixedBufferPool *arrays, FixedBufferPool *buffers);
    ~Put();

    std::size_t size() const;

    int status;
};

}

}

#endif