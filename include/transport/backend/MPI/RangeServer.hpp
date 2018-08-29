#ifndef TRANSPORT_MPI_RANGE_SERVER_HPP
#define TRANSPORT_MPI_RANGE_SERVER_HPP

#include <atomic>
#include <pthread.h>
#include <unistd.h>
#include <vector>

#include <mpi.h>

#include "hxhim/struct.h"
#include "transport/transport.hpp"
#include "utils/FixedBufferPool.hpp"

namespace Transport {
namespace MPI {

class RangeServer {
    public:
        static int init(hxhim_t *hx, const std::size_t listener_count);
        static void destroy();

        /**
         * Function that will be used to listen for MPI messages
         */
        static void *listener_thread(void *data);

    private:
        static int recv(void **data, std::size_t *len);
        static int send(const int dst, void *data, const std::size_t len);

        static int Flush(MPI_Request &req);
        static int Flush(MPI_Request &req, MPI_Status &status);

        static hxhim_t *hx_;
        static std::atomic_bool *running_;
        static std::vector<pthread_t> listeners_;
        static pthread_mutex_t mutex_;
        static FixedBufferPool *packed_;
        static FixedBufferPool *arrays_;
        static FixedBufferPool *buffers_;
};

}
}

#endif
