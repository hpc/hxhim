#ifndef HXHIM_TRANSPORT_ENDPOINT_GROUP
#define HXHIM_TRANSPORT_ENDPOINT_GROUP

#include "mlog2.h"
#include "mlogfacs2.h"
#include <mpi.h>

#include "mdhim_constants.h"
#include "mdhim_private.h"
#include "transport.hpp"
#include "MemoryManagers.hpp"
#include "MPIEndpointBase.hpp"
#include "MPIPacker.hpp"
#include "MPIUnpacker.hpp"

/**
 * MPIEndpointGroup
 * Collective communication endpoint implemented with MPI
 */
class MPIEndpointGroup : virtual public TransportEndpointGroup, virtual public MPIEndpointBase {
    public:
        MPIEndpointGroup(mdhim_private_t *mdp, volatile int &shutdown);
        ~MPIEndpointGroup();

        /** @description Enqueue a BPut requests to multiple endpoints  */
        TransportBRecvMessage *BPut(const int num_rangesrvs, TransportBPutMessage **bpm_list);

        /** @description Enqueue a BGet request to multiple endpoints  */
        TransportBGetRecvMessage *BGet(const int num_rangesrvs, TransportBGetMessage **bgm_list);

        /** @description Bulk Delete to multiple endpoints   */
        TransportBRecvMessage *BDelete(const int num_rangesrvs, TransportBDeleteMessage **bpm_list);

    private:
        /** @description Function used by BPUT and BDELETE for sending and receiving messages */
        TransportBRecvMessage *return_brm(const int num_rangesrvs, TransportMessage **messages);

        /** @description Function used by BGET for sending and receiving messages             */
        TransportBGetRecvMessage *return_bgrm(const int num_rangesrvs, TransportMessage **messages);

        /**
         * Functions that perform the actual MPI calls
         */
        int only_send_all_rangesrv_work(void **messages, int *sizes, int *dsts, const int num_srvs);
        int send_all_rangesrv_work(TransportMessage **messages, const int num_srvs);

        int only_receive_all_client_responses(int *srcs, int nsrcs, void ***recvbufs, int **sizebufs);
        int receive_all_client_responses(int *srcs, int nsrcs, TransportMessage ***messages);

        mdhim_private_t *mdp_;
};


#endif
