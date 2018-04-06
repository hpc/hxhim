#ifndef HXHIM_TRANSPORT_ENDPOINT_GROUP
#define HXHIM_TRANSPORT_ENDPOINT_GROUP

#include "mlog2.h"
#include "mlogfacs2.h"
#include <mpi.h>

#include "mdhim_constants.h"
#include "mdhim_struct.h"
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
        MPIEndpointGroup(mdhim_t *md, volatile int &shutdown);
        ~MPIEndpointGroup();

        /** @description Enqueue a BPut requests to multiple endpoints  */
        TransportBRecvMessage *BPut(const int num_rangesrvs, TransportBPutMessage **bpm_list);

        /** @description Enqueue a BGet request to multiple endpoints  */
        TransportBGetRecvMessage *BGet(const int num_rangesrvs, TransportBGetMessage **bgm_list);

        /** @description Bulk Delete to multiple endpoints   */
        TransportBRecvMessage *BDelete(const int num_rangesrvs, TransportBDeleteMessage **bpm_list);

    private:
        /**
         * Common function for sending and receiving bulk operation message
         */
        template <typename Recv_t>
        Recv_t *do_operation(const int num_rangesrvs, TransportMessage **messages);

        /**
         * Functions that perform the actual MPI calls
         */
        int only_send_all_rangesrv_work(void **messages, int *sizes, int *dsts, const int num_srvs);
        int send_all_rangesrv_work(TransportMessage **messages, int * dsts, const int num_srvs);

        int only_receive_all_client_responses(int *srcs, int nsrcs, void ***recvbufs, int **sizebufs);
        int receive_all_client_responses(int *srcs, int nsrcs, TransportMessage ***messages);

        mdhim_t *md_;
};

/**
 * do_operation
 *
 * @param index     the index used to find where messages should go
 * @param messages  an array of messages to send
 * @return a linked list of return messages
*/
template <typename Recv_t>
Recv_t *MPIEndpointGroup::do_operation(const int num_rangesrvs, TransportMessage **messages) {
    // get the actual number of servers
    int num_srvs = 0;
    int *srvs = new int[num_rangesrvs]();
    for (int i = 0; i < num_rangesrvs; i++) {
        if (!messages[i]) {
            continue;
        }

        srvs[num_srvs] = messages[i]->dst;
        num_srvs++;
    }

    if (!num_srvs) {
        delete [] srvs;
        return nullptr;
    }

    // send the messages
    if (send_all_rangesrv_work(messages, srvs, num_rangesrvs) != MDHIM_SUCCESS) {
        delete [] srvs;
        return nullptr;
    }

    // wait for responses
    Recv_t **recv_list = new Recv_t *[num_srvs]();
    if (receive_all_client_responses(srvs, num_srvs, (TransportMessage ***)&recv_list) != MDHIM_SUCCESS) {
        delete [] srvs;
        return nullptr;
    }

    // convert the responses into a list
    Recv_t *head = nullptr;
    Recv_t *tail = nullptr;
    for (int i = 0; i < num_srvs; i++) {
        Recv_t *recv = recv_list[i];
        if (!recv) {
            //Skip this as the message doesn't exist
            continue;
        }

        //Build the linked list to return
        recv->next = nullptr;
        if (!head) {
            head = recv;
            tail = recv;
        } else {
            tail->next = recv;
            tail = recv;
        }
    }

    delete [] recv_list;
    delete [] srvs;

    // Return response list
    return head;
}

#endif
