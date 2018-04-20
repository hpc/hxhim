#ifndef HXHIM_TRANSPORT_MPI_ENDPOINT
#define HXHIM_TRANSPORT_MPI_ENDPOINT

#include "mlog2.h"
#include "mlogfacs2.h"
#include <mpi.h>

#include "mdhim_constants.h"
#include "transport.hpp"
#include "MemoryManagers.hpp"
#include "MPIEndpointBase.hpp"
#include "MPIPacker.hpp"
#include "MPIUnpacker.hpp"

/**
 * MPIEndpoint
 * Point-to-Point communication endpoint implemented with MPI
 */
class MPIEndpoint : virtual public TransportEndpoint, virtual public MPIEndpointBase {
    public:
        /** Create a TransportEndpoint for a specified process rank */
        MPIEndpoint(const MPI_Comm comm,
                    const int remote_rank,
                    FixedBufferPool *fbp,
                    volatile int &shutdown);

        /** Destructor */
        ~MPIEndpoint() {}

        TransportRecvMessage *Put(const TransportPutMessage *message);
        TransportGetRecvMessage *Get(const TransportGetMessage *message);
        TransportRecvMessage *Delete(const TransportDeleteMessage *message);

    private:
        /**
         * Functions that perform the actual MPI calls
         */
        int send_rangesrv_work(const void *buf, const int size);
        int receive_client_response(void **buf, int *size);

        /**
         * do_operation
         * A function containing the common calls used by Put, Get, and Delete
         *
         * @tparam message the message being sent
         * @treturn the response from the range server
         */
        template <typename Send_t, typename Recv_t>
        Recv_t *do_operation(const Send_t *message) {
            void *sendbuf = nullptr;
            int sendsize = 0;
            void *recvbuf = nullptr;
            int recvsize = 0;
            Recv_t *response = nullptr;

            // the result of this series of function calls does not matter
            (void)
                ((MPIPacker::pack(comm_, message, &sendbuf, &sendsize, fbp_) == MDHIM_SUCCESS) &&  // pack the message
                 (send_rangesrv_work(sendbuf, sendsize)                      == MDHIM_SUCCESS) &&  // send the message
                 (receive_client_response(&recvbuf, &recvsize)               == MDHIM_SUCCESS) &&  // receive the response
                 (MPIUnpacker::unpack(comm_, &response, recvbuf, recvsize)   == MDHIM_SUCCESS));   // unpack the response

            fbp_->release(sendbuf);
            fbp_->release(recvbuf);

            return dynamic_cast<Recv_t *>(response);
        }

        void Flush(MPI_Request *req);

        const int remote_rank_;
        volatile int &shutdown_;
};

#endif
