#ifndef TRANSPORT_THALLIUM_RANGE_SERVER_HPP
#define TRANSPORT_THALLIUM_RANGE_SERVER_HPP

#include <thallium.hpp>

#include "hxhim/struct.h"
#include "hxhim/options.h"
#include "transport/transport.hpp"
#include "transport/backend/Thallium/Utilities.hpp"

namespace Transport {
namespace Thallium {

class RangeServer : virtual public ::Transport::RangeServer {
    public:
        /**
         * RPC name called by the client to send and receive data
         */
        static const std::string PROCESS_RPC_NAME;
        static const std::string CLEANUP_RPC_NAME;

        RangeServer(hxhim_t *hx, thallium::engine *engine);
        ~RangeServer();

        // access RPCs using these functions
        const thallium::remote_procedure &process() const;
        const thallium::remote_procedure &cleanup() const;

    private:
        hxhim_t *hx;
        thallium::engine *engine;  // not owned by RangeServer
        int rank;

        void process(const thallium::request &req, const std::size_t req_len, thallium::bulk &bulk);
        const thallium::remote_procedure process_rpc;

        void cleanup(const thallium::request &req, uintptr_t addr);
        const thallium::remote_procedure cleanup_rpc;
};

}
}

#endif
