#ifndef HXHIM_TRANSPORT_MPI_ENDPOINT
#define HXHIM_TRANSPORT_MPI_ENDPOINT

#include "mlog2.h"
#include "mlogfacs2.h"
#include <mpi.h>

#include "mdhim_constants.h"
#include "transport.hpp"
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
        MPIEndpoint(const MPI_Comm comm, const int remote_rank, volatile int &shutdown);

        /** Destructor */
        ~MPIEndpoint() {}

        int AddPutRequest(const TransportPutMessage *message);
        int AddGetRequest(const TransportGetMessage *message);

        int AddPutReply(TransportRecvMessage **message);
        int AddGetReply(TransportGetRecvMessage **message);

    private:
        /**
         * Functions that perform the actual MPI calls
         */
        int send_rangesrv_work(const void *buf, const int size);
        int receive_client_response(void **buf, int *size);

        const int remote_rank_;
};

#endif
