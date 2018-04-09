#include "MPIEndpointGroup.hpp"

MPIEndpointGroup::MPIEndpointGroup(mdhim_private_t *mdp, volatile int &shutdown)
  : TransportEndpointGroup(),
    MPIEndpointBase(mdp->mdhim_comm, shutdown),
    mdp_(mdp)
{}

MPIEndpointGroup::~MPIEndpointGroup() {}

TransportBRecvMessage *MPIEndpointGroup::BPut(const int num_rangesrvs, TransportBPutMessage **bpm_list) {
    TransportMessage **messages = new TransportMessage *[num_rangesrvs]();
    for(int i = 0; i < num_rangesrvs; i++) {
        messages[i] = bpm_list[i];
    }

    return return_brm(num_rangesrvs, messages);
}

TransportBGetRecvMessage *MPIEndpointGroup::BGet(const int num_rangesrvs, TransportBGetMessage **bgm_list) {
    TransportMessage **messages = new TransportMessage *[num_rangesrvs]();
    for(int i = 0; i < num_rangesrvs; i++) {
        messages[i] = bgm_list[i];
    }

    return return_bgrm(num_rangesrvs, messages);
}

TransportBRecvMessage *MPIEndpointGroup::BDelete(const int num_rangesrvs, TransportBDeleteMessage **bdm_list) {
    TransportMessage **messages = new TransportMessage *[num_rangesrvs]();
    for(int i = 0; i < num_rangesrvs; i++) {
        messages[i] = bdm_list[i];
    }

    return return_brm(num_rangesrvs, messages);
}

/**
 * return_brm
 *
 * @param num_rangesrvs the number of range servers there are
 * @param messages      an array of messages to send
 * @return a linked list of return messages
*/
TransportBRecvMessage *MPIEndpointGroup::return_brm(const int num_rangesrvs, TransportMessage **messages) {
    // get the actual number of servers
    int num_srvs = 0;
    int *srvs = new int[num_rangesrvs]();
    for (int i = 0; i < num_rangesrvs; i++) {
        if (!messages[i]) {
            continue;
        }

        // store server IDs to receive frome
        srvs[num_srvs] = messages[i]->dst;
        num_srvs++;
    }

    if (!num_srvs) {
        delete [] srvs;
        return nullptr;
    }

    // send the messages
    if (send_all_rangesrv_work(messages, num_rangesrvs) != MDHIM_SUCCESS) {
        delete [] srvs;
        return nullptr;
    }

    // wait for responses
    TransportMessage **recv_list = new TransportMessage *[num_srvs]();
    if (receive_all_client_responses(srvs, num_srvs, &recv_list) != MDHIM_SUCCESS) {
        delete [] srvs;
        return nullptr;
    }

    // convert the responses into a list
    TransportBRecvMessage *head = nullptr;
    TransportBRecvMessage *tail = nullptr;
    for (int i = 0; i < num_srvs; i++) {
        TransportRecvMessage *rm = dynamic_cast<TransportRecvMessage *>(recv_list[i]);
        if (!rm) {
            //Skip this as the message doesn't exist
            continue;
        }

        TransportBRecvMessage *brm = new TransportBRecvMessage();
        brm->error = rm->error;
        brm->src = rm->src;
        brm->dst = rm->dst;
        brm->next = nullptr;
        delete rm;

        //Build the linked list to return
        if (!head) {
            head = brm;
            tail = brm;
        } else {
            tail->next = brm;
            tail = brm;
        }
    }

    delete [] recv_list;
    delete [] srvs;

    // Return response list
    return head;
}

/**
 * return_brm
 *
 * @param num_rangesrvs the number of range servers there are
 * @param messages      an array of messages to send
 * @return a linked list of return messages
*/
TransportBGetRecvMessage *MPIEndpointGroup::return_bgrm(const int num_rangesrvs, TransportMessage **messages) {
    // get the actual number of servers
    int num_srvs = 0;
    int *srvs = new int[num_rangesrvs]();
    for (int i = 0; i < num_rangesrvs; i++) {
        if (!messages[i]) {
            continue;
        }

        // store server IDs to receive frome
        srvs[num_srvs] = messages[i]->dst;
        num_srvs++;
    }

    if (!num_srvs) {
        delete [] srvs;
        return nullptr;
    }

    // send the messages
    if (send_all_rangesrv_work(messages, num_rangesrvs) != MDHIM_SUCCESS) {
        delete [] srvs;
        return nullptr;
    }

    // wait for responses
    TransportMessage **bgrm_list = new TransportMessage *[num_srvs]();
    if (receive_all_client_responses(srvs, num_srvs, &bgrm_list) != MDHIM_SUCCESS) {
        delete [] srvs;
        return nullptr;
    }

    // convert the responses into a list
    TransportBGetRecvMessage *head = nullptr;
    TransportBGetRecvMessage *tail = nullptr;
    for (int i = 0; i < num_srvs; i++) {
        TransportBGetRecvMessage *bgrm = dynamic_cast<TransportBGetRecvMessage *>(bgrm_list[i]);
        if (!bgrm) {
            //Skip this as the message doesn't exist
            continue;
        }

        //Build the linked list to return
        bgrm->next = nullptr;
        if (!head) {
            head = bgrm;
            tail = bgrm;
        } else {
            tail->next = bgrm;
            tail = bgrm;
        }
    }

    delete [] bgrm_list;
    delete [] srvs;

    // Return response list
    return head;
}

/**
 * only_send_all_rangesrv_work
 * Sends multiple messages (char buffers)  simultaneously and waits for them to all complete
 *
 * @param md       main MDHIM struct
 * @param messages double pointer to array of packed messages to send
 * @param sizes    size of each message
 * @param num_srvs number of different servers
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int MPIEndpointGroup::only_send_all_rangesrv_work(void **messages, int *sizes, int *dsts, const int num_srvs) {
    int return_code = MDHIM_ERROR;
    MPI_Request **size_reqs = new MPI_Request *[num_srvs]();
    MPI_Request **reqs = new MPI_Request *[num_srvs]();
    int num_msgs = 0;
    int ret = MDHIM_SUCCESS;

    //Send all messages at once
    for (int i = 0; i < num_srvs; i++) {
        void *mesg = messages[i];
        if (!mesg) {
            continue;
        }

        size_reqs[i] = new MPI_Request();

        // send size
        pthread_mutex_lock(&mdp_->mdhim_comm_lock);
        return_code = MPI_Isend(&sizes[i], 1, MPI_INT, dsts[i], RANGESRV_WORK_SIZE_MSG,
                                mdp_->mdhim_comm, size_reqs[i]);
        pthread_mutex_unlock(&mdp_->mdhim_comm_lock);

        if (return_code != MPI_SUCCESS) {
            ret = MDHIM_ERROR;
        }

        reqs[i] = new MPI_Request();

        // send data
        pthread_mutex_lock(&mdp_->mdhim_comm_lock);
        return_code = MPI_Isend(messages[i], sizes[i], MPI_PACKED, dsts[i], RANGESRV_WORK_MSG,
                                mdp_->mdhim_comm, reqs[i]);
        pthread_mutex_unlock(&mdp_->mdhim_comm_lock);

        if (return_code != MPI_SUCCESS) {
            ret = MDHIM_ERROR;
        }

        num_msgs++;
    }

    //Wait for messages to complete
    const int total_msgs = num_msgs * 2;
    int done = 0;
    while (done != total_msgs) {
        int flag;
        MPI_Status status;

        for (int i = 0; i < num_srvs; i++) {
            if (!size_reqs[i]) {
                continue;
            }

            pthread_mutex_lock(&mdp_->mdhim_comm_lock);
            ret = MPI_Test(size_reqs[i], &flag, &status);
            pthread_mutex_unlock(&mdp_->mdhim_comm_lock);

            if (flag) {
                delete size_reqs[i];
                size_reqs[i] = nullptr;
                done++;
            }
        }
        for (int i = 0; i < num_srvs; i++) {
            if (!reqs[i]) {
                continue;
            }

            pthread_mutex_lock(&mdp_->mdhim_comm_lock);
            ret = MPI_Test(reqs[i], &flag, &status);
            pthread_mutex_unlock(&mdp_->mdhim_comm_lock);

            if (flag) {
                delete reqs[i];
                reqs[i] = nullptr;
                done++;
            }
        }

        if (done != total_msgs) {
            usleep(100);
        }
    }

    delete [] reqs;
    delete [] size_reqs;

    return ret;
}

/**
 * send_all_rangesrv_work
 * Sends multiple messages simultaneously and waits for them to all complete
 *
 * @param md       main MDHIM struct
 * @param messages double pointer to array of messages to send
 * @param sizes    size of each message
 * @param num_srvs number of different servers
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int MPIEndpointGroup::send_all_rangesrv_work(TransportMessage **messages, const int num_srvs) {
    int return_code = MDHIM_SUCCESS;
    void **sendbufs = Memory::FBP_MEDIUM::Instance().acquire<void *>(num_srvs);
    int *sizes = new int[num_srvs]();             // size of each sendbuf
    int *dsts = new int[num_srvs]();

    // encode each mesage
    for(int i = 0; i < num_srvs; i++) {
        if (MPIPacker::any(comm_, messages[i], sendbufs + i, sizes + i) != MDHIM_SUCCESS) {
            Memory::FBP_MEDIUM::Instance().release(sendbufs[i]);
            sendbufs[i] = nullptr;
            sizes[i] = 0;
            continue;
        }

        dsts[i] = messages[i]->dst;
    }

    // send all of the messages at once
    int ret = only_send_all_rangesrv_work(sendbufs, sizes, dsts, num_srvs);

    // cleanup
    for (int i = 0; i < num_srvs; i++) {
        Memory::FBP_MEDIUM::Instance().release(sendbufs[i]);
    }

    Memory::FBP_MEDIUM::Instance().release(sendbufs);
    delete [] dsts;
    delete [] sizes;

    return ret;
}

int MPIEndpointGroup::only_receive_all_client_responses(int *srcs, int nsrcs, void ***recvbufs, int **sizebufs) {
    MPI_Status status;
    int return_code;
    int ret = MDHIM_SUCCESS;
    MPI_Request **reqs = new MPI_Request*[nsrcs]();

    *recvbufs = Memory::FBP_MEDIUM::Instance().acquire<void *>(nsrcs);
    *sizebufs = new int[nsrcs]();

    // Receive a size message from the servers in the list
    for (int i = 0; i < nsrcs; i++) {
        reqs[i] = new MPI_Request();

        pthread_mutex_lock(&mdp_->mdhim_comm_lock);
        return_code = MPI_Irecv((*sizebufs) + i, 1, MPI_INT,
                                srcs[i], CLIENT_RESPONSE_SIZE_MSG,
                                mdp_->mdhim_comm, reqs[i]);
        pthread_mutex_unlock(&mdp_->mdhim_comm_lock);

        // If the receive did not succeed then return the error code back
        if ( return_code != MPI_SUCCESS ) {
            ret = MDHIM_ERROR;
        }
    }

    // Wait for size messages to complete
    int done = 0;
    while (done != nsrcs) {
        for (int i = 0; i < nsrcs; i++) {
            // if there is a request
            if (reqs[i]) {
                int flag = 0;

                // test the request
                pthread_mutex_lock(&mdp_->mdhim_comm_lock);
                return_code = MPI_Test(reqs[i], &flag, &status);
                pthread_mutex_unlock(&mdp_->mdhim_comm_lock);

                // if the request completed, add 1
                if (flag) {
                    delete reqs[i];
                    reqs[i] = nullptr;
                    done++;
                }
            }
        }

        if (done != nsrcs) {
            usleep(100);
        }
    }

    done = 0;
    for (int i = 0; i < nsrcs; i++) {
        // Receive a message from the servers in the list
        (*recvbufs)[i] = Memory::FBP_MEDIUM::Instance().acquire((*sizebufs)[i]);
        reqs[i] = new MPI_Request();

        pthread_mutex_lock(&mdp_->mdhim_comm_lock);
        return_code = MPI_Irecv((*recvbufs)[i], (*sizebufs)[i], MPI_PACKED,
                                srcs[i], CLIENT_RESPONSE_MSG,
                                mdp_->mdhim_comm, reqs[i]);
        pthread_mutex_unlock(&mdp_->mdhim_comm_lock);

        // If the receive did not succeed then return the error code back
        if ( return_code != MPI_SUCCESS ) {
            ret = MDHIM_ERROR;
        }
    }

    //Wait for messages to complete
    while (done != nsrcs) {
        for (int i = 0; i < nsrcs; i++) {
            if (!reqs[i]) {
                continue;
            }

            int flag = 0;

            pthread_mutex_lock(&mdp_->mdhim_comm_lock);
            return_code = MPI_Test(reqs[i], &flag, &status);
            pthread_mutex_unlock(&mdp_->mdhim_comm_lock);

            if (!flag) {
              continue;
            }
            delete reqs[i];
            reqs[i] = nullptr;
            done++;
        }

        if (done != nsrcs) {
            usleep(100);
        }
    }

    delete [] reqs;

    return ret;
}

int MPIEndpointGroup::receive_all_client_responses(int *srcs, int nsrcs, TransportMessage ***messages) {
    void **recvbufs = nullptr;
    int *sizebufs = nullptr;
    int ret = MDHIM_ERROR;

    if ((ret = only_receive_all_client_responses(srcs, nsrcs, &recvbufs, &sizebufs)) == MDHIM_SUCCESS) {
        for (int i = 0; i < nsrcs; i++) {
            ret = MPIUnpacker::any(mdp_->mdhim_comm, (*messages) + i, recvbufs[i], sizebufs[i]);
            Memory::FBP_MEDIUM::Instance().release(recvbufs[i]);
        }
    }

    Memory::FBP_MEDIUM::Instance().release(recvbufs);
    delete [] sizebufs;

    return ret;
}
