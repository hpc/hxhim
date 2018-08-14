#ifndef TRANSPORT_PUT_MESSAGE_HPP
#define TRANSPORT_PUT_MESSAGE_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "transport/Request.hpp"
#include "transport/Response.hpp"
#include "transport/Single.hpp"
#include "transport/constants.h"

namespace Transport {

namespace Request {

struct Put final : Request, Single {
    Put();
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
    Put();
    ~Put();

    std::size_t size() const;

    int status;
};

}

}

#endif
