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

        RangeServer(hxhim_t *hx, Engine_t &engine);
        ~RangeServer();

        // access RPCs using these functions
        RPC_t process() const;
        RPC_t cleanup() const;

    private:
        hxhim_t *hx;
        Engine_t engine;
        int rank;

        void process(const thallium::request &req, const std::size_t req_len, thallium::bulk &bulk);
        RPC_t process_rpc;

        void cleanup(const thallium::request &req, uintptr_t addr);
        RPC_t cleanup_rpc;
};

}
}

#endif
