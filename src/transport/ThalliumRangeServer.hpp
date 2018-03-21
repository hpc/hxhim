#ifndef HXHIM_TRANSPORT_THALLIUM_RANGE_SERVER
#define HXHIM_TRANSPORT_THALLIUM_RANGE_SERVER

#include <pthread.h>
#include <unistd.h>

#include <thallium.hpp>

#include "mdhim_private.h"
#include "transport.hpp"
#include "ThalliumPacker.hpp"
#include "ThalliumUnpacker.hpp"

class ThalliumRangeServer {
    public:
        static void init(mdhim_private_t *mdp);

        /**
         * Function that will be used to listen for Thallium  messages
         */
        static void *listener_thread(void *data);

        /**
         * Fnction that will be used to send responses back to the client
         */
        static int send_locally_or_remote(mdhim_t *md, const int dest, TransportMessage *message);

    private:
        int receive_rangesrv_work(const std::string &data);

        static mdhim_private_t *mdp_;
        static pthread_mutex_t mutex_;
};

#endif
