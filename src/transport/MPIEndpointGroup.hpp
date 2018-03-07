#ifndef HXHIM_TRANSPORT_ENDPOINT_GROUP
#define HXHIM_TRANSPORT_ENDPOINT_GROUP

#include "mlog2.h"
#include "mlogfacs2.h"
#include <mpi.h>

#include "mdhim_constants.h"
#include "transport.hpp"
#include "MPIAddress.hpp"
#include "MPITransportBase.hpp"
#include "MPIPacker.hpp"
#include "MPIUnpacker.hpp"

/**
 * MPIEndpointGroup
 * Collective communication endpoint implemented with MPI
*/
class MPIEndpointGroup : virtual public TransportEndpointGroup, virtual public MPITransportBase {
    public:
        MPIEndpointGroup(MPI_Comm comm, volatile int &shutdown);
        ~MPIEndpointGroup();

        int AddBPutRequest(TransportBPutMessage **messages, int num_srvs);
        int AddBGetRequest(TransportBGetMessage **messages, int num_srvs);

        int AddBPutReply(const TransportAddress *srcs, int nsrcs, TransportRecvMessage **messages);
        int AddBGetReply(const TransportAddress *srcs, int nsrcs, TransportBGetRecvMessage **messages);

        const TransportAddress *Address() const;

    private:
        const MPIAddress address_;
};

#endif
