#ifndef TRANSPORT_THALLIUM_RANGE_SERVER_HPP
#define TRANSPORT_THALLIUM_RANGE_SERVER_HPP

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

#include "hxhim/struct.h"
#include "transport/transport.hpp"

namespace Transport {
namespace Thallium {

class RangeServer {
    public:
        /**
         * RPC name called by the client to send and receive data
         */
        static const std::string CLIENT_TO_RANGE_SERVER_NAME;

        static void init(hxhim_t *hx);
        static void destroy();

        static void process(const thallium::request &req, const std::string &data);

    private:
        static hxhim_t *hx_;
};

}
}

#endif
