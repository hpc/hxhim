#ifndef HXHIM_TRANSPORT_THALLIUM_ENDPOINT
#define HXHIM_TRANSPORT_THALLIUM_ENDPOINT

#include <memory>
#include <mutex>

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

#include "mdhim_constants.h"
#include "mdhim_private.h"
#include "mdhim_struct.h"
#include "transport.hpp"
#include "MemoryManagers.hpp"
#include "ThalliumPacker.hpp"
#include "ThalliumRangeServer.hpp"
#include "ThalliumUnpacker.hpp"

/**
 * ThalliumAddress
 * Point-to-Point communication endpoint implemented with thallium
 */
class ThalliumEndpoint : virtual public TransportEndpoint {
    public:
        ThalliumEndpoint(thallium::engine *engine,
                         thallium::remote_procedure *rpc,
                         thallium::endpoint *ep);
        ~ThalliumEndpoint();

        TransportRecvMessage *Put(const TransportPutMessage *message);
        TransportGetRecvMessage *Get(const TransportGetMessage *message);

    // private:
        static std::mutex mutex_;                 // mutex for engine_ and rpc_
        static std::size_t count_;                // used to figure out when to destroy engine_ and rpc_
        static thallium::engine *engine_;
        static thallium::remote_procedure *rpc_;  // client to server RPC
        thallium::endpoint *ep_;                  // the server the RPC will be called on
};

#endif
