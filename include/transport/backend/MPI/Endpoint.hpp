#ifndef TRANSPORT_MPI_ENDPOINT_HPP
#define TRANSPORT_MPI_ENDPOINT_HPP

#include <atomic>
#include <type_traits>

#include <mpi.h>

#include "transport/backend/MPI/EndpointBase.hpp"
#include "transport/backend/MPI/Packer.hpp"
#include "transport/backend/MPI/Unpacker.hpp"
#include "transport/transport.hpp"
#include "utils/FixedBufferPool.hpp"
#include "utils/enable_if_t.hpp"
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
                 volatile std::atomic_bool &running,
                 std::shared_ptr<FixedBufferPool> packed,
                 FixedBufferPool *responses,
                 FixedBufferPool *arrays,
                 FixedBufferPool *buffers);

        /** Destructor */
        ~Endpoint() {}

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
                    (Packer::pack(comm, message, &sendbuf, &sendsize, packed.get()) == TRANSPORT_SUCCESS) &&  // pack the message
                    (send(sendbuf, sendsize)                                        == TRANSPORT_SUCCESS) &&  // send the message
                    (recv(&recvbuf, &recvsize)                                      == TRANSPORT_SUCCESS) &&  // receive the response
                    (Unpacker::unpack(comm, &response, recvbuf, recvsize,
                                      responses, arrays, buffers)                   == TRANSPORT_SUCCESS)     // unpack the response
                );

            packed->release(sendbuf, sendsize);
            packed->release(recvbuf, recvsize);

            return dynamic_cast<Recv_t *>(response);
        }

        const int remote_rank;
        volatile std::atomic_bool &running;

        FixedBufferPool *responses;
        FixedBufferPool *arrays;
        FixedBufferPool *buffers;
};

}
}

#endif
