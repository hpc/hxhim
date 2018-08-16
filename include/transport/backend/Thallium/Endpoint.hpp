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
                 const Endpoint_t &ep);
        ~Endpoint();

        /** @description Send a Put to this endpoint */
        Response::Put *Put(const Request::Put *message);

        /** @description Send a Get to this endpoint */
        Response::Get *Get(const Request::Get *message);

        /** @description Send a Delete to this endpoint */
        Response::Delete *Delete(const Request::Delete *message);

        /** @description Send a Histogram to this endpoint */
        Response::Histogram *Histogram(const Request::Histogram *message);

    private:
        /**
         * do_operation
         * A function containing the common calls used by Put, Get, and Delete
         *
         * @tparam message the message being sent
         * @treturn the response from the range server
         */
        template <typename Recv_t, typename Send_t, typename = std::enable_if<std::is_base_of<Request::Request,   Send_t>::value &&
                                                                              std::is_base_of<Single,             Send_t>::value &&
                                                                              std::is_base_of<Response::Response, Recv_t>::value &&
                                                                              std::is_base_of<Single,             Recv_t>::value> >
        Recv_t *do_operation(const Send_t *message) {
            std::lock_guard<std::mutex> lock(mutex_);

            std::string buf;
            if (Packer::pack(message, buf) != TRANSPORT_SUCCESS) {
                return nullptr;
            }

            const std::string response = rpc_->on(*ep_)(buf);

            Recv_t *ret = nullptr;
            if (Unpacker::unpack(&ret, response) != TRANSPORT_SUCCESS) {
                return nullptr;
            }

            return ret;
        }

        static std::mutex mutex_;  // mutex for engine_ and rpc_

        Engine_t engine_;          // declare engine first so it is destroyed last
        RPC_t rpc_;                // client to server RPC
        Endpoint_t ep_;            // the server the RPC will be called on
};

}
}

#endif
