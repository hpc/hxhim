#ifndef TRANSPORT_LOCAL_ENDPOINT_HPP
#define TRANSPORT_LOCAL_ENDPOINT_HPP

#include "hxhim/struct.h"
#include "transport/transport.hpp"
#include "utils/FixedBufferPool.hpp"

namespace Transport {
namespace local {

/**
 * Local Endpoint
 */
class Endpoint : virtual public ::Transport::Endpoint {
    public:
        /** Create a TransportEndpoint for a specified process rank */
        Endpoint(hxhim_t *hx);

        /** Destructor */
        ~Endpoint() {}

        /** @description Send a Put to this endpoint */
        Response::Put *Put(const Request::Put *message);

        /** @description Send a Get to this endpoint */
        Response::Get *Get(const Request::Get *message);

        /** @description Send a Delete to this endpoint */
        Response::Delete *Delete(const Request::Delete *message);

        /** @description Send a Histogram to this endpoint */
        Response::Histogram *Histogram(const Request::Histogram *message);

    private:
        hxhim_t *hx;
};

}
}

#endif
