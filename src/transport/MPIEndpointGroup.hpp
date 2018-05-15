#ifndef MDHIM_TRANSPORT_ENDPOINT_GROUP_HPP
#define MDHIM_TRANSPORT_ENDPOINT_GROUP_HPP

#include <map>
#include <pthread.h>

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
 * MPIEndpointGroup
 * Collective communication endpoint implemented with MPI
 */
class MPIEndpointGroup : virtual public TransportEndpointGroup, virtual public MPIEndpointBase {
    public:
        MPIEndpointGroup(const MPI_Comm comm, pthread_mutex_t mutex,
                         FixedBufferPool *fbp);

        ~MPIEndpointGroup();

        /** @description Add a mapping from a unique ID to a MPI rank */
        void AddID(const int id, const int rank);

        /** @ description Remove a unique ID from the map */
        void RemoveID(const int id);

        /** @description Enqueue a BPut requests to multiple endpoints  */
        TransportBRecvMessage *BPut(const std::size_t num_rangesrvs, TransportBPutMessage **bpm_list);

        /** @description Enqueue a BGet request to multiple endpoints  */
        TransportBGetRecvMessage *BGet(const std::size_t num_rangesrvs, TransportBGetMessage **bgm_list);

        /** @description Bulk Delete to multiple endpoints   */
        TransportBRecvMessage *BDelete(const std::size_t num_rangesrvs, TransportBDeleteMessage **bpm_list);

    private:
        /**
         * Functions that perform the actual MPI calls
         */
        int only_send_all_rangesrv_work(void **messages, std::size_t *sizes, int *dsts, const std::size_t num_srvs);
        int send_all_rangesrv_work(TransportMessage **messages, const std::size_t num_srvs);

        int only_receive_all_client_responses(int *srcs, std::size_t nsrcs, void ***recvbufs, std::size_t **sizebufs);
        int receive_all_client_responses(int *srcs, std::size_t nsrcs, TransportMessage ***messages);

        /**
         * return_msgs
         * Send TransportB*Messages and waits for their responses.
         * The responses are chained together into a list.
         *
         * This function takes ownership of the messages array and deallocates it.
         *
         * @param num_rangesrvs the number of range servers there are
         * @param messages      an array of messages to send
         * @treturn a linked list of return messages
         */
        template<typename T>
        T *return_msgs(const std::size_t num_rangesrvs, TransportMessage **messages) {
            int *srvs = nullptr;
            std::size_t num_srvs = 0;
            if (!(num_srvs = get_num_srvs(messages, num_rangesrvs, &srvs))) {
                delete [] messages;
                return nullptr;
            }

            // send the messages
            if (send_all_rangesrv_work(messages, num_rangesrvs) != MDHIM_SUCCESS) {
                delete [] srvs;
                delete [] messages;
                return nullptr;
            }

            // wait for responses
            TransportMessage **recv_list = new TransportMessage *[num_srvs]();
            if (receive_all_client_responses(srvs, num_srvs, &recv_list) != MDHIM_SUCCESS) {
                delete [] srvs;
                delete [] messages;
                return nullptr;
            }

            // convert the responses into a list
            T *head = nullptr;
            T *tail = nullptr;
            for (std::size_t i = 0; i < num_srvs; i++) {
                T *brm = dynamic_cast<T *>(recv_list[i]);
                if (brm) {
                    //Build the linked list to return
                    if (!head) {
                        head = brm;
                        tail = brm;
                    } else {
                        tail->next = brm;
                        tail = brm;
                    }
                }
            }

            delete [] recv_list;
            delete [] srvs;
            delete [] messages;

            // Return response list
            return head;
        }


        pthread_mutex_t mutex_;

        /** @description Mapping from unique ids to MPI ranks */
        std::map<int, int> ranks_;
};

#endif
