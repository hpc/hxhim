#ifndef TRANSPORT_DELETE_MESSAGE_HPP
#define TRANSPORT_DELETE_MESSAGE_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "transport/Messages/Request.hpp"
#include "transport/Messages/Response.hpp"
#include "transport/Messages/Single.hpp"
#include "transport/constants.hpp"

namespace Transport {

namespace Request {

struct Delete final : Request, Single {
    Delete(FixedBufferPool *arrays, FixedBufferPool *buffers);
    ~Delete();

    std::size_t size() const;

    void *subject;
    std::size_t subject_len;
    void *predicate;
    std::size_t predicate_len;
};

}

namespace Response {

struct Delete final : Response, Single {
    Delete(FixedBufferPool *arrays, FixedBufferPool *buffers);
    ~Delete();

    std::size_t size() const;

    int status;
};

}

}

#endif
