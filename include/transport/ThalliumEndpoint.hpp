#ifndef MDHIM_TRANSPORT_THALLIUM_ENDPOINT_HPP
#define MDHIM_TRANSPORT_THALLIUM_ENDPOINT_HPP

#include <memory>
#include <mutex>
#include <stdexcept>

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

#include "ThalliumPacker.hpp"
#include "ThalliumUnpacker.hpp"
#include "ThalliumUtilities.hpp"
#include "mdhim/constants.h"
#include "mdhim/private.h"
#include "mdhim/struct.h"
#include "transport.hpp"

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
        TransportRecvMessage *Delete(const TransportDeleteMessage *message);

    private:
        /**
         * do_operation
         * A function containing the common calls used by Put, Get, and Delete
         *
         * @tparam message the message being sent
         * @treturn the response from the range server
         */
        template <typename Send_t, typename Recv_t>
        Recv_t *do_operation(const Send_t *message) {
            std::lock_guard<std::mutex> lock(mutex_);

            if (!rpc_ || !ep_) {
                return nullptr;
            }

            std::string buf;
            if (ThalliumPacker::pack(message, buf) != MDHIM_SUCCESS) {
                return nullptr;
            }

            const std::string response = rpc_->on(*ep_)(buf);

            Recv_t *ret = nullptr;
            if (ThalliumUnpacker::unpack(&ret, response) != MDHIM_SUCCESS) {
                return nullptr;
            }

            return ret;
        }

        static std::mutex mutex_;     // mutex for engine_ and rpc_

        Thallium::Engine_t engine_;   // declare engine first so it is destroyed last
        Thallium::RPC_t rpc_;         // client to server RPC
        Thallium::Endpoint_t ep_;     // the server the RPC will be called on
};

#endif
