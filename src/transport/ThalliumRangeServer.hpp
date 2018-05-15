#ifndef MDHIM_TRANSPORT_THALLIUM_RANGE_SERVER_HPP
#define MDHIM_TRANSPORT_THALLIUM_RANGE_SERVER_HPP

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

#include "mdhim_private.h"
#include "range_server.h"
#include "transport.hpp"
#include "work_item.h"
#include "ThalliumPacker.hpp"
#include "ThalliumUnpacker.hpp"

class ThalliumRangeServer {
    public:
        /**
         * RPC name called by the client to send and receive data
         */
        static const std::string CLIENT_TO_RANGE_SERVER_NAME;

        static void init(mdhim_private_t *mdp);
        static void destroy();

        /**
         * Function that will be defined by the client and called by the range server
         */
        static int send_client_response(work_item_t *item, TransportMessage *message, volatile int &shutdown);

    // private:
        static void receive_rangesrv_work(const thallium::request &req, const std::string &data);

        static mdhim_private_t *mdp_;
};

#endif
