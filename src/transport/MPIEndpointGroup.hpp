#ifndef HXHIM_TRANSPORT_ENDPOINT_GROUP
#define HXHIM_TRANSPORT_ENDPOINT_GROUP

#include <map>
#include <pthread.h>

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
    MPIEndpointGroup(const MPI_Comm comm, pthread_mutex_t mutex, volatile int &shutdown);
        ~MPIEndpointGroup();

        /** @description Add a mapping from a unique ID to a MPI rank */
        void AddID(const int id, const int rank);

        /** @ description Remove a unique ID from the map */
        void RemoveID(const int id);

        /** @description Enqueue a BPut requests to multiple endpoints  */
        TransportBRecvMessage *BPut(const int num_rangesrvs, TransportBPutMessage **bpm_list);

        /** @description Enqueue a BGet request to multiple endpoints  */
        TransportBGetRecvMessage *BGet(const int num_rangesrvs, TransportBGetMessage **bgm_list);

        /** @description Bulk Delete to multiple endpoints   */
        TransportBRecvMessage *BDelete(const int num_rangesrvs, TransportBDeleteMessage **bpm_list);

    private:
        /** @description Sends messages that result in TransportBRecvMessage responses */
        TransportBRecvMessage *return_brm(const int num_rangesrvs, TransportMessage **messages);

        /** @description Sends messages that result in TransportBGetRecvMessage responses */
        TransportBGetRecvMessage *return_bgrm(const int num_rangesrvs, TransportMessage **messages);

        /**
         * Functions that perform the actual MPI calls
         */
        int only_send_all_rangesrv_work(void **messages, int *sizes, int *dsts, const int num_srvs);
        int send_all_rangesrv_work(TransportMessage **messages, const int num_srvs);

        int only_receive_all_client_responses(int *srcs, int nsrcs, void ***recvbufs, int **sizebufs);
        int receive_all_client_responses(int *srcs, int nsrcs, TransportMessage ***messages);

        pthread_mutex_t mutex_;

        /** @description Mapping from unique ids to MPI ranks */
        std::map<int, int> ranks_;
};

#endif
