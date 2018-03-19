#ifndef HXHIM_TRANSPORT_MPI_ENDPOINT
#define HXHIM_TRANSPORT_MPI_ENDPOINT

#include "mlog2.h"
#include "mlogfacs2.h"
#include <mpi.h>

#include "mdhim_constants.h"
#include "transport.hpp"
#include "MPIAddress.hpp"
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
        MPIEndpoint(MPI_Comm comm, volatile int &shutdown);

        /** Destructor */
        ~MPIEndpoint() {}

        int AddPutRequest(const TransportPutMessage *message);
        int AddGetRequest(const TransportBGetMessage *message); // no-op

        int AddPutReply(const TransportAddress *src, TransportRecvMessage **message);
        int AddGetReply(const TransportAddress *src, TransportBGetRecvMessage ***messages); // no-op

        std::size_t PollForMessage(std::size_t timeoutSecs);

        std::size_t WaitForMessage(std::size_t timeoutSecs);

        int Flush(); // no-op

        const TransportAddress *Address() const;

    private:
        const MPIAddress address_;
};

#endif
