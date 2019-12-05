#if HXHIM_HAVE_THALLIUM

#ifndef TRANSPORT_THALLIUM_ENDPOINT_HPP
#define TRANSPORT_THALLIUM_ENDPOINT_HPP

#include <memory>
#include <mutex>
#include <stdexcept>

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

#include "transport/backend/Thallium/Packer.hpp"
#include "transport/backend/Thallium/Unpacker.hpp"
#include "transport/backend/Thallium/Utilities.hpp"
#include "transport/transport.hpp"
#include "utils/memory.hpp"

namespace Transport {
namespace Thallium {

/**
 * Endpoint
 * Point-to-Point communication endpoint implemented with thallium
 */
class Endpoint : virtual public ::Transport::Endpoint {
    public:
        Endpoint(const Engine_t &engine,
                 const RPC_t &rpc,
                 const std::size_t buffer_size,
                 const Endpoint_t &ep);
        ~Endpoint();

        /** @description Send a Put to this endpoint */
        Response::Put *communicate(const Request::Put *message);

        /** @description Send a Get to this endpoint */
        Response::Get *communicate(const Request::Get *message);

        /** @description Send a Get2 to this endpoint */
        Response::Get2 *communicate(const Request::Get2 *message);

        /** @description Send a Delete to this endpoint */
        Response::Delete *communicate(const Request::Delete *message);

        /** @description Send a Histogram to this endpoint */
        Response::Histogram *communicate(const Request::Histogram *message);

    private:
        static std::mutex mutex;          // mutex for engine_ and rpc_

        Engine_t engine;                  // declare engine first so it is destroyed last
        RPC_t rpc;                        // client to server RPC
        const std::size_t buffer_size;    // size of the rpc buffer
        Endpoint_t ep;                    // the server the RPC will be called on
};

}
}

#endif

#endif
