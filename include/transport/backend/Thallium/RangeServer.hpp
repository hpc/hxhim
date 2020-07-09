#ifndef TRANSPORT_THALLIUM_RANGE_SERVER_HPP
#define TRANSPORT_THALLIUM_RANGE_SERVER_HPP

#include <thallium.hpp>

#include "hxhim/struct.h"
#include "hxhim/options.h"
#include "transport/transport.hpp"
#include "transport/backend/Thallium/Utilities.hpp"

namespace Transport {
namespace Thallium {

class RangeServer {
    public:
        /**
         * RPC name called by the client to send and receive data
         */
        static const std::string PROCESS_RPC_NAME;
        static const std::string CLEANUP_RPC_NAME;

        static void init(hxhim_t *hx, const Engine_t &engine);
        static void destroy();

        static void process(const thallium::request &req, const std::size_t req_len, thallium::bulk &bulk);
        static void cleanup(const thallium::request &req, uintptr_t addr);

    private:
        static hxhim_t *hx_;
        static Engine_t engine_;
        static std::size_t bufsize_;
};

}
}

#endif
