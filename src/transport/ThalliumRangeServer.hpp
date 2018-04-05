#ifndef HXHIM_TRANSPORT_THALLIUM_RANGE_SERVER
#define HXHIM_TRANSPORT_THALLIUM_RANGE_SERVER

#include <pthread.h>

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

#include "mdhim_private.h"
#include "range_server.h"
#include "transport.hpp"
#include "MPIInstance.hpp"
#include "MemoryManagers.hpp"
#include "ThalliumPacker.hpp"
#include "ThalliumUnpacker.hpp"

class ThalliumRangeServer {
    public:
        /**
         * RPC name called by the client to send and receive data
         */
        static const std::string CLIENT_TO_RANGE_SERVER_NAME;
        static const std::string RANGE_SERVER_TO_CLIENT_NAME;

        static void init(mdhim_private_t *mdp);

        /**
         * Function that will be defined by the client and called by the range server
         */
        static int send_client_response(int dest, TransportMessage *message, volatile int &shutdown);

    // private:
        static void receive_rangesrv_work(const thallium::request &req, const std::string &data);

        static mdhim_private_t *mdp_;
};

#endif
