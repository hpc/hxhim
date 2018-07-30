#ifndef TRANSPORT_SYNC_MESSAGE_HPP
#define TRANSPORT_SYNC_MESSAGE_HPP

#include "transport/Request.hpp"
#include "transport/Response.hpp"
#include "transport/Single.hpp"

namespace Transport {

namespace Request {

struct Sync final : Request, Single {
    Sync();

    std::size_t size() const;
};

}

namespace Response {

struct Sync final : Response, Single {
    Sync();

    std::size_t size() const;
};

}

}

#endif
