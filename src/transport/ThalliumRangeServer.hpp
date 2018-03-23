#ifndef HXHIM_TRANSPORT_THALLIUM_RANGE_SERVER
#define HXHIM_TRANSPORT_THALLIUM_RANGE_SERVER

#include <iostream>
#include <pthread.h>
#include <unistd.h>

#include <thallium.hpp>

#include "mdhim_private.h"
#include "transport.hpp"
#include "ThalliumPacker.hpp"
#include "ThalliumUnpacker.hpp"

class ThalliumRangeServer {
    public:
        static const std::string CLIENT_TO_RANGE_SERVER_NAME;
        static const std::string RANGE_SERVER_NAME_TO_CLIENT;

        static void init(mdhim_private_t *mdp, const std::string &address);

        /**
         * Function that will be used to listen for Thallium  messages
         */
        static void *listener_thread(void *data);

        /**
         * Fnction that will be used to send responses back to the client
         */
        static int send_locally_or_remote(mdhim_t *md, const int dest, TransportMessage *message);

    private:
        static void receive_rangesrv_work(const thallium::request &req, const std::string &data);
        static std::string send_client_response(const thallium::request &req);

        static mdhim_private_t *mdp_;
        static std::string address_;
        static pthread_mutex_t mutex_;
};

#endif
