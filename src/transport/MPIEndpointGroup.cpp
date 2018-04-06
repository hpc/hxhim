#include "MPIEndpointGroup.hpp"

MPIEndpointGroup::MPIEndpointGroup(mdhim_t *md, volatile int &shutdown)
  : TransportEndpointGroup(),
    MPIEndpointBase(md->p->mdhim_comm, shutdown),
    md_(md)
{}

MPIEndpointGroup::~MPIEndpointGroup() {}

TransportBRecvMessage *MPIEndpointGroup::BPut(const int num_rangesrvs, TransportBPutMessage **bpm_list) {
    return do_operation<TransportBRecvMessage>(num_rangesrvs, (TransportMessage **)bpm_list);
}

TransportBGetRecvMessage *MPIEndpointGroup::BGet(const int num_rangesrvs, TransportBGetMessage **bgm_list) {
    return do_operation<TransportBGetRecvMessage>(num_rangesrvs, (TransportMessage **)bgm_list);
}

TransportBRecvMessage *MPIEndpointGroup::BDelete(const int num_rangesrvs, TransportBDeleteMessage **bdm_list) {
    return do_operation<TransportBRecvMessage>(num_rangesrvs, (TransportMessage **)bdm_list);
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
        pthread_mutex_lock(&md_->p->mdhim_comm_lock);
        return_code = MPI_Isend(&sizes[i], 1, MPI_INT, dsts[i], RANGESRV_WORK_SIZE_MSG,
                    md_->p->mdhim_comm, size_reqs[i]);
        pthread_mutex_unlock(&md_->p->mdhim_comm_lock);

        if (return_code != MPI_SUCCESS) {
            ret = MDHIM_ERROR;
        }

        reqs[i] = new MPI_Request();

        // send data
        pthread_mutex_lock(&md_->p->mdhim_comm_lock);
        return_code = MPI_Isend(messages[i], sizes[i], MPI_PACKED, dsts[i], RANGESRV_WORK_MSG,
                    md_->p->mdhim_comm, reqs[i]);
        pthread_mutex_unlock(&md_->p->mdhim_comm_lock);

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

            pthread_mutex_lock(&md_->p->mdhim_comm_lock);
            ret = MPI_Test(size_reqs[i], &flag, &status);
            pthread_mutex_unlock(&md_->p->mdhim_comm_lock);

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

            pthread_mutex_lock(&md_->p->mdhim_comm_lock);
            ret = MPI_Test(reqs[i], &flag, &status);
            pthread_mutex_unlock(&md_->p->mdhim_comm_lock);

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
int MPIEndpointGroup::send_all_rangesrv_work(TransportMessage **messages, int *dsts, const int num_srvs) {
    int return_code = MDHIM_SUCCESS;
    void **sendbufs = Memory::FBP_MEDIUM::Instance().acquire<void *>(num_srvs);
    int *sizes = new int[num_srvs]();             // size of each sendbuf

    int ret = MDHIM_SUCCESS;
    // encode each mesage
    for(int i = 0; i < num_srvs; i++) {
        if (MPIPacker::any(comm_, messages[i], sendbufs + i, sizes + i) != MDHIM_SUCCESS) {
            ret = MDHIM_ERROR;
            Memory::FBP_MEDIUM::Instance().release(sendbufs[i]);
            sendbufs[i] = nullptr;
            sizes[i] = 0;
            continue;
        }
    }

    // send all of the messages at once
    ret = only_send_all_rangesrv_work(sendbufs, sizes, dsts, num_srvs);

    // cleanup
    for (int i = 0; i < num_srvs; i++) {
        Memory::FBP_MEDIUM::Instance().release(sendbufs[i]);
    }

    Memory::FBP_MEDIUM::Instance().release(sendbufs);
    delete [] sizes;

    return ret;
}

int MPIEndpointGroup::only_receive_all_client_responses(int *srcs, int nsrcs, void ***recvbufs, int **sizebufs) {
    MPI_Status status;
    int return_code;
    int ret = MDHIM_SUCCESS;
    int done = 0;
    MPI_Request **reqs = new MPI_Request*[nsrcs]();

    *sizebufs = new int[nsrcs]();
    *recvbufs = new void *[nsrcs]();

    // Receive a size message from the servers in the list
    for (int i = 0; i < nsrcs; i++) {
        reqs[i] = new MPI_Request();

        pthread_mutex_lock(&md_->p->mdhim_comm_lock);
        return_code = MPI_Irecv(&sizebufs[i], 1, MPI_INT,
                                srcs[i], CLIENT_RESPONSE_SIZE_MSG,
                                md_->p->mdhim_comm, reqs[i]);
        pthread_mutex_unlock(&md_->p->mdhim_comm_lock);

        // If the receive did not succeed then return the error code back
        if ( return_code != MPI_SUCCESS ) {
            ret = MDHIM_ERROR;
        }
    }

    // Wait for size messages to complete
    while (done != nsrcs) {
        for (int i = 0; i < nsrcs; i++) {
            if (!reqs[i]) {
                continue;
            }

            int flag = 0;

            pthread_mutex_lock(&md_->p->mdhim_comm_lock);
            return_code = MPI_Test(reqs[i], &flag, &status);
            pthread_mutex_unlock(&md_->p->mdhim_comm_lock);

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

    done = 0;
    for (int i = 0; i < nsrcs; i++) {
        // Receive a message from the servers in the list
        *recvbufs[i] = ::operator new(*sizebufs[i]);
        reqs[i] = new MPI_Request();

        pthread_mutex_lock(&md_->p->mdhim_comm_lock);
        return_code = MPI_Irecv(*recvbufs[i], *sizebufs[i], MPI_PACKED,
                                srcs[i], CLIENT_RESPONSE_MSG,
                                md_->p->mdhim_comm, reqs[i]);
        pthread_mutex_unlock(&md_->p->mdhim_comm_lock);

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

            pthread_mutex_lock(&md_->p->mdhim_comm_lock);
            return_code = MPI_Test(reqs[i], &flag, &status);
            pthread_mutex_unlock(&md_->p->mdhim_comm_lock);

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
            *(*messages + i) = nullptr;
            ret = MPIUnpacker::any(md_->p->mdhim_comm, messages[i], recvbufs[i], sizebufs[i]);
            ::operator delete(recvbufs[i]);
        }
    }

    delete [] recvbufs;
    delete [] sizebufs;

    return ret;
}
