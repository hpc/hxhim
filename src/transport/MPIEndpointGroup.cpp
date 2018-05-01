#include "MPIEndpointGroup.hpp"

MPIEndpointGroup::MPIEndpointGroup(const MPI_Comm comm, pthread_mutex_t mutex,
                                   FixedBufferPool *fbp)
  : TransportEndpointGroup(),
    MPIEndpointBase(comm, fbp),
    mutex_(mutex),
    ranks_()
{}

MPIEndpointGroup::~MPIEndpointGroup() {}

/**
 * AddID
 * Add a mapping from a unique ID to a MPI rank
 *
 * @param id the unique ID associated with the rank
 * @param rank the rank associated with the unique ID
 */
void MPIEndpointGroup::AddID(const int id, const int rank) {
    ranks_[id] = rank;
}

/**
 * RemoveID
 * Remove a mapping from a unique ID to a rank
 * If the unique ID is not found, nothing is done
 *
 * @param id the unique ID to remove
 */
void MPIEndpointGroup::RemoveID(const int id) {
    ranks_.erase(id);
}

/**
 * BPut
 *
 * @param num_rangesrvs the total number of range servers
 * @param bpm_list the list of BPUT messages to send
 * @return a linked list of response messages, or nullptr
 */
TransportBRecvMessage *MPIEndpointGroup::BPut(const std::size_t num_rangesrvs, TransportBPutMessage **bpm_list) {
    return return_msgs<TransportBRecvMessage>(num_rangesrvs, convert_to_base(num_rangesrvs, bpm_list));
}

/**
 * BGet
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list the list of BGET messages to send
 * @return a linked list of response messages, or nullptr
 */
TransportBGetRecvMessage *MPIEndpointGroup::BGet(const std::size_t num_rangesrvs, TransportBGetMessage **bgm_list) {
    return return_msgs<TransportBGetRecvMessage>(num_rangesrvs, convert_to_base(num_rangesrvs, bgm_list));
}

/**
 * BDelete
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list the list of BDELETE messages to send
 * @return a linked list of response messages, or nullptr
 */
TransportBRecvMessage *MPIEndpointGroup::BDelete(const std::size_t num_rangesrvs, TransportBDeleteMessage **bdm_list) {
    return return_msgs<TransportBRecvMessage>(num_rangesrvs, convert_to_base(num_rangesrvs, bdm_list));
}

/**
 * only_send_all_rangesrv_work
 * Sends multiple messages (char buffers)  simultaneously and waits for them to all complete
 *
 * @param messages double pointer to array of packed messages to send
 * @param sizes    size of each message
 * @param dsts     which range server each message is going to
 * @param num_srvs number of different servers
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int MPIEndpointGroup::only_send_all_rangesrv_work(void **messages, std::size_t *sizes, int *dsts, const std::size_t num_srvs) {
    int return_code = MDHIM_ERROR;
    MPI_Request **size_reqs = new MPI_Request *[num_srvs]();
    MPI_Request **reqs = new MPI_Request *[num_srvs]();
    std::size_t num_msgs = 0;
    int ret = MDHIM_SUCCESS;

    //Send all messages at once
    for (std::size_t i = 0; i < num_srvs; i++) {
        void *mesg = messages[i];
        if (!mesg) {
            continue;
        }

        std::map<int, int>::const_iterator dst_it = ranks_.find(dsts[i]);
        if (dst_it != ranks_.end()) {
            size_reqs[i] = new MPI_Request();

            // send size
            pthread_mutex_lock(&mutex_);
            return_code = MPI_Isend(&sizes[i], sizeof(*sizes), MPI_CHAR, dst_it->second, RANGESRV_WORK_SIZE_MSG,
                                    comm_, size_reqs[i]);
            pthread_mutex_unlock(&mutex_);

            if (return_code != MPI_SUCCESS) {
                ret = MDHIM_ERROR;
            }

            reqs[i] = new MPI_Request();

            // send data
            pthread_mutex_lock(&mutex_);
            return_code = MPI_Isend(messages[i], sizes[i], MPI_CHAR, dst_it->second, RANGESRV_WORK_MSG,
                                    comm_, reqs[i]);
            pthread_mutex_unlock(&mutex_);

            if (return_code != MPI_SUCCESS) {
                ret = MDHIM_ERROR;
            }

            num_msgs++;
        }
    }

    //Wait for messages to complete
    const std::size_t total_msgs = num_msgs * 2;
    std::size_t done = 0;
    while (done != total_msgs) {
        int flag;
        MPI_Status status;

        for (std::size_t i = 0; i < num_srvs; i++) {
            if (!size_reqs[i]) {
                continue;
            }

            pthread_mutex_lock(&mutex_);
            ret = MPI_Test(size_reqs[i], &flag, &status);
            pthread_mutex_unlock(&mutex_);

            if (flag) {
                delete size_reqs[i];
                size_reqs[i] = nullptr;
                done++;
            }
        }
        for (std::size_t i = 0; i < num_srvs; i++) {
            if (!reqs[i]) {
                continue;
            }

            pthread_mutex_lock(&mutex_);
            ret = MPI_Test(reqs[i], &flag, &status);
            pthread_mutex_unlock(&mutex_);

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
 * @param messages double pointer to array of messages to send
 * @param num_srvs number of different servers
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int MPIEndpointGroup::send_all_rangesrv_work(TransportMessage **messages, const std::size_t num_srvs) {
    void **sendbufs = new void *[num_srvs]();
    std::size_t *sizebufs = new std::size_t[num_srvs]();
    int *dsts = new int[num_srvs]();

    // encode each mesage
    for(std::size_t i = 0; i < num_srvs; i++) {
        if (MPIPacker::any(comm_, messages[i], sendbufs + i, sizebufs + i, fbp_) != MDHIM_SUCCESS) {
            fbp_->release(sendbufs[i]);
            sendbufs[i] = nullptr;
            sizebufs[i] = 0;
            continue;
        }

        dsts[i] = messages[i]->dst;
    }

    // send all of the messages at once
    int ret = only_send_all_rangesrv_work(sendbufs, sizebufs, dsts, num_srvs);

    // cleanup
    for (std::size_t i = 0; i < num_srvs; i++) {
        fbp_->release(sendbufs[i]);
    }

    delete [] dsts;
    delete [] sendbufs;
    delete [] sizebufs;

    return ret;
}

int MPIEndpointGroup::only_receive_all_client_responses(int *srcs, std::size_t nsrcs, void ***recvbufs, std::size_t **sizebufs) {
    MPI_Status status;
    int return_code;
    int ret = MDHIM_SUCCESS;
    MPI_Request **reqs = new MPI_Request*[nsrcs]();
    std::size_t num_endpoints = 0;

    *recvbufs = new void *[nsrcs]();
    *sizebufs = new std::size_t[nsrcs]();

    // Receive a size message from the servers in the list
    for (std::size_t i = 0; i < nsrcs; i++) {
        std::map<int, int>::const_iterator src_it = ranks_.find(srcs[i]);
        if (src_it != ranks_.end()) {
            reqs[i] = new MPI_Request();
            pthread_mutex_lock(&mutex_);
            return_code = MPI_Irecv((*sizebufs) + i, sizeof(**sizebufs), MPI_CHAR,
                                    src_it->second, CLIENT_RESPONSE_SIZE_MSG,
                                    comm_, reqs[i]);
            pthread_mutex_unlock(&mutex_);

            // If the receive did not succeed then return the error code back
            if ( return_code != MPI_SUCCESS ) {
               ret = MDHIM_ERROR;
            }
            else {
                num_endpoints++;
            }
        }
    }

    // Wait for size messages to complete
    std::size_t done = 0;
    while (done != num_endpoints) {
        for (std::size_t i = 0; i < nsrcs; i++) {
            // if there is a request
            if (reqs[i]) {
                int flag = 0;

                // test the request
                pthread_mutex_lock(&mutex_);
                MPI_Test(reqs[i], &flag, &status);
                pthread_mutex_unlock(&mutex_);

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
    for (std::size_t i = 0; i < nsrcs; i++) {
        std::map<int, int>::const_iterator src_it = ranks_.find(srcs[i]);
        if (src_it != ranks_.end()) {
            // Receive a message from the servers in the list
            (*recvbufs)[i] = fbp_->acquire((*sizebufs)[i]);
            reqs[i] = new MPI_Request();

            pthread_mutex_lock(&mutex_);
            return_code = MPI_Irecv((*recvbufs)[i], (*sizebufs)[i], MPI_CHAR,
                                    src_it->second, CLIENT_RESPONSE_MSG,
                                    comm_, reqs[i]);
            pthread_mutex_unlock(&mutex_);

            // If the receive did not succeed then return the error code back
            if ( return_code != MPI_SUCCESS ) {
                ret = MDHIM_ERROR;
            }
        }
    }

    //Wait for messages to complete
    while (done != num_endpoints) {
        for (std::size_t i = 0; i < nsrcs; i++) {
            if (!reqs[i]) {
                continue;
            }

            int flag = 0;

            pthread_mutex_lock(&mutex_);
            MPI_Test(reqs[i], &flag, &status);
            pthread_mutex_unlock(&mutex_);

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

int MPIEndpointGroup::receive_all_client_responses(int *srcs, std::size_t nsrcs, TransportMessage ***messages) {
    void **recvbufs = nullptr;
    std::size_t *sizebufs = nullptr;
    int ret = MDHIM_SUCCESS;

    if ((ret = only_receive_all_client_responses(srcs, nsrcs, &recvbufs, &sizebufs)) == MDHIM_SUCCESS) {
        for (std::size_t i = 0; i < nsrcs; i++) {
            if (recvbufs[i]) {
                ret = MPIUnpacker::any(comm_, (*messages) + i, recvbufs[i], sizebufs[i]);
                fbp_->release(recvbufs[i]);
            }
        }
    }

    delete [] recvbufs;
    delete [] sizebufs;

    return ret;
}
