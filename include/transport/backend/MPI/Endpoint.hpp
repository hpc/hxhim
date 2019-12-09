#ifndef TRANSPORT_MPI_ENDPOINT_HPP
#define TRANSPORT_MPI_ENDPOINT_HPP

#include <atomic>
#include <type_traits>

#include <mpi.h>

#include "transport/backend/MPI/EndpointBase.hpp"
#include "transport/transport.hpp"
#include "utils/enable_if_t.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

namespace Transport {
namespace MPI {

/**
 * MPIEndpoint
 * Point-to-Point communication endpoint implemented with MPI
 */
class Endpoint : virtual public ::Transport::Endpoint, virtual public EndpointBase {
    public:
        /** Create a TransportEndpoint for a specified process rank */
        Endpoint(const MPI_Comm comm,
                 const int remote_rank,
                 volatile std::atomic_bool &running);

        /** Destructor */
        ~Endpoint() {}

        /** @description Send a Put to this endpoint */
        Response::Put *communicate(const Request::Put *message);

        /** @description Send a Get to this endpoint */
        Response::Get *communicate(const Request::Get *message);

        /** @description Send a Get2 to this endpoint */
        Response::Get2 *communicate(const Request::Get2 *message);

        /** @description Send a Delete to this endpoint */
        Response::Delete *communicate(const Request::Delete *message);

        /** @description Send a Histogram to this endpoint */
        Response::Histogram *communicate(const Request::Histogram *message);

    private:
        /**
         * Functions that perform the actual MPI calls
         */
        int send(void *data, const std::size_t len); // send to the range server
        int recv(void **data, std::size_t *len);     // receive from the range server
        int Flush(MPI_Request &req);

        /**
         * do_operation
         * A function containing the common calls used by Put, Get, and Delete
         *
         * @tparam message the message being sent
         * @treturn the response from the range server
         */
        template <typename Send_t, typename Recv_t, typename = enable_if_t<std::is_base_of<Request::Request,   Send_t>::value &&
                                                                           std::is_base_of<Single,             Send_t>::value &&
                                                                           std::is_base_of<Response::Response, Recv_t>::value &&
                                                                           std::is_base_of<Single,             Recv_t>::value> >
        Recv_t *do_operation(const Send_t *message) {
            void *sendbuf = nullptr;
            std::size_t sendsize = 0;
            void *recvbuf = nullptr;
            std::size_t recvsize = 0;
            Recv_t *response = nullptr;

            // the result of this series of function calls does not matter
            (void)
                (
                    (Packer::pack(message, &sendbuf, &sendsize)     == TRANSPORT_SUCCESS) &&  // pack the message
                    (send(sendbuf, sendsize)                        == TRANSPORT_SUCCESS) &&  // send the message
                    (recv(&recvbuf, &recvsize)                      == TRANSPORT_SUCCESS) &&  // receive the response
                    (Unpacker::unpack(&response, recvbuf, recvsize) == TRANSPORT_SUCCESS)     // unpack the response
                );

            dealloc(sendbuf);
            dealloc(recvbuf);

            return dynamic_cast<Recv_t *>(response);
        }

        const int remote_rank;
        volatile std::atomic_bool &running;
};

}
}

#endif
