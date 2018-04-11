#ifndef HXHIM_TRANSPORT_THALLIUM_ENDPOINT
#define HXHIM_TRANSPORT_THALLIUM_ENDPOINT

#include <memory>
#include <mutex>
#include <stdexcept>

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

#include "mdhim_constants.h"
#include "mdhim_private.h"
#include "mdhim_struct.h"
#include "transport.hpp"
#include "ThalliumPacker.hpp"
#include "ThalliumUnpacker.hpp"
#include "ThalliumUtilities.hpp"

/**
 * ThalliumEndpoint
 * Point-to-Point communication endpoint implemented with thallium
 */
class ThalliumEndpoint : virtual public TransportEndpoint {
    public:
        ThalliumEndpoint(const Thallium::Engine_t &engine,
                         const Thallium::RPC_t &rpc,
                         const Thallium::Endpoint_t &ep);
        ~ThalliumEndpoint();

        TransportRecvMessage *Put(const TransportPutMessage *message);
        TransportGetRecvMessage *Get(const TransportGetMessage *message);

    private:
        static std::mutex mutex_;     // mutex for engine_ and rpc_

        Thallium::Engine_t engine_;   // declare engine first so it is destroyed last
        Thallium::RPC_t rpc_;         // client to server RPC
        Thallium::Endpoint_t ep_;     // the server the RPC will be called on
};

#endif
