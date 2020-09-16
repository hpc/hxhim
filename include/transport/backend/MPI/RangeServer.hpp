#ifndef TRANSPORT_MPI_RANGE_SERVER_HPP
#define TRANSPORT_MPI_RANGE_SERVER_HPP

#include <thread>
#include <vector>

#include <mpi.h>

#include "hxhim/struct.h"
#include "hxhim/options.h"
#include "transport/transport.hpp"

namespace Transport {
namespace MPI {

class RangeServer : virtual public ::Transport::RangeServer {
    public:
        RangeServer(hxhim_t *hx, const std::size_t listener_count);
        ~RangeServer();

    private:
        /**
         * Function that will be used to listen for MPI messages
         */
        void listener_thread();

        int recv(void **data, std::size_t *len);
        int send(const int dst, void *data, const std::size_t len);

        int Flush(MPI_Request &req);
        int Flush(MPI_Request &req, MPI_Status &status);

        hxhim_t *hx;
        std::vector<std::thread> listeners;
};

}
}

#endif
