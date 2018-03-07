#include "MPITransportBase.hpp"

pthread_mutex_t MPITransportBase::mutex_ = PTHREAD_MUTEX_INITIALIZER;

MPITransportBase::MPITransportBase(const MPI_Comm comm, volatile int &shutdown)
    : comm_(comm),
      shutdown_(shutdown)
{
    if (comm_ == MPI_COMM_NULL) {
        throw std::runtime_error("Received MPI_COMM_NULL as communicator");
    }

    if (MPI_Comm_rank(comm_, &rank_) != MPI_SUCCESS) {
        throw std::runtime_error("Failed to get rank within MPI communicator");
    }

    if (MPI_Comm_size(comm_, &size_) != MPI_SUCCESS) {
        throw std::runtime_error("Failed to get the size of the MPI communicator");
    }
}

MPITransportBase::~MPITransportBase() {}

MPI_Comm MPITransportBase::Comm() const {
    return comm_;
}

int MPITransportBase::Rank() const {
    return rank_;
}

int MPITransportBase::Size() const {
    return size_;
}

/**
 * send_rangesrv_work
 * Sends a packed message (char buffer) to the range server at the given destination
 *
 * @param dest    destination to send to
 * @param buf     data to be sent to dest
 * @param size    size of data to be sent to dest
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int MPITransportBase::send_rangesrv_work(int dest, const void *buf, const int size) {
    int return_code = MDHIM_ERROR;
    MPI_Request req;

    //Send the size of the message
    pthread_mutex_lock(&mutex_);
    return_code = MPI_Isend(&size, sizeof(size), MPI_CHAR, dest, RANGESRV_WORK_SIZE_MSG, comm_, &req);
    pthread_mutex_unlock(&mutex_);
    Flush(&req);

    if (return_code != MPI_SUCCESS) {
        mlog(MPI_CRIT, "Rank %d - "
             "Error sending work size message in send_rangesrv_work",
             rank_);
        return MDHIM_ERROR;
    }

    //Send the message
    pthread_mutex_lock(&mutex_);
    return_code = MPI_Isend(buf, size, MPI_PACKED, dest, RANGESRV_WORK_MSG, comm_, &req);
    pthread_mutex_unlock(&mutex_);
    Flush(&req);

    if (return_code != MPI_SUCCESS) {
        mlog(MPI_CRIT, "Rank %d - "
             "Error sending work message in send_rangesrv_work",
             rank_);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

/**
 * send_all_rangesrv_work
 * Sends multiple messages (char buffers)  simultaneously and waits for them to all complete
 *
 * @param messages double pointer to array of packed messages to send
 * @param sizes    size of each message
 * @param num_srvs number of different servers
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int MPITransportBase::send_all_rangesrv_work(void **messages, int *sizes, int *dests, int num_srvs) {
    int return_code = MDHIM_ERROR;
    MPI_Request **size_reqs = new MPI_Request*[num_srvs]();
    MPI_Request **reqs = new MPI_Request*[num_srvs]();
    int num_msgs = 0;
    int ret = MDHIM_SUCCESS;

    //Send all messages at once
    for (int i = 0; i < num_srvs; i++) {
        if (!messages[i]) {
            continue;
        }

        size_reqs[i] = new MPI_Request();

        // send size
        pthread_mutex_lock(&mutex_);
        return_code = MPI_Isend(sizes + i, sizeof(*sizes), MPI_CHAR, dests[i], RANGESRV_WORK_SIZE_MSG, comm_, size_reqs[i]);
        pthread_mutex_unlock(&mutex_);

        if (return_code != MPI_SUCCESS) {
            mlog(MPI_CRIT, "Rank %d - "
                 "Error sending work message in send_rangesrv_work",
                 rank_);
            ret = MDHIM_ERROR;
        }

        // send data
        reqs[i] = new MPI_Request();
        pthread_mutex_lock(&mutex_);
        return_code = MPI_Isend(messages[i], sizes[i], MPI_PACKED, dests[i], RANGESRV_WORK_MSG, comm_, reqs[i]);
        pthread_mutex_unlock(&mutex_);

        if (return_code != MPI_SUCCESS) {
            mlog(MPI_CRIT, "Rank %d - "
                 "Error sending work message in send_rangesrv_work",
                 rank_);
            ret = MDHIM_ERROR;
        }

        num_msgs++;
    }

    //Wait for messages to complete sending
    const int total_msgs = num_msgs * 2;
    int done = 0;
    while (done != total_msgs) {
        int flag;
        MPI_Status status;

        for (int i = 0; i < num_srvs; i++) {
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
        for (int i = 0; i < num_srvs; i++) {
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

    delete [] size_reqs;
    delete [] reqs;

    return ret;
}

/**
 * send_client_response
 * Sends a buffer to a client
 *
 * @param dest    destination to send to
 * @param sendbuf double pointer to packed message
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int MPITransportBase::send_client_response(int dest, int *sizebuf, void **sendbuf, MPI_Request **size_req, MPI_Request **msg_req) {
    int return_code = 0;
    int mtype;
    int ret = MDHIM_SUCCESS;

    //Send the size message
    *size_req = (MPI_Request*)malloc(sizeof(MPI_Request));

    pthread_mutex_lock(&mutex_);
    return_code = MPI_Isend(sizebuf, sizeof(*sizebuf), MPI_CHAR, dest, CLIENT_RESPONSE_SIZE_MSG, comm_, *size_req);
    pthread_mutex_unlock(&mutex_);

    if (return_code != MPI_SUCCESS) {
        mlog(MPI_CRIT, "Rank %d - "
             "Error sending client response message size in send_client_response",
             rank_);
        ret = MDHIM_ERROR;
        free(*size_req);
        *size_req = nullptr;
    }

    *msg_req = (MPI_Request*)malloc(sizeof(MPI_Request));
    //Send the actual message

    pthread_mutex_lock(&mutex_);
    return_code = MPI_Isend(*sendbuf, *sizebuf, MPI_PACKED, dest, CLIENT_RESPONSE_MSG, comm_, *msg_req);
    pthread_mutex_unlock(&mutex_);

    if (return_code != MPI_SUCCESS) {
        mlog(MPI_CRIT, "Rank %d - "
             "Error sending client response message in send_client_response",
             rank_);
        ret = MDHIM_ERROR;
        free(*msg_req);
        *msg_req = nullptr;
    }

    return ret;
}

/**
 * receive_client_response message
 * Receives a message from the given source
 *
 * @param md      in   main MDHIM struct
 * @param src     in   source to receive from
 * @param message out  double pointer for message received
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int MPITransportBase::receive_client_response(int src, void **recvbuf, int *recvsize) {
    int return_code;
    MPI_Request req;

    // Receive the size of the message
    pthread_mutex_lock(&mutex_);
    return_code = MPI_Irecv(recvsize, sizeof(*recvsize), MPI_CHAR, src, CLIENT_RESPONSE_SIZE_MSG, comm_, &req);
    pthread_mutex_unlock(&mutex_);
    Flush(&req);

    // If the receive did not succeed then return the error code back
    if (return_code != MPI_SUCCESS) {
        mlog(MPI_CRIT, "Rank %d - "
             "Error receiving message in receive_client_response",
             rank_);
        return MDHIM_ERROR;
    }

    // allocate space for the message
    if (!(*recvbuf = calloc(*recvsize, sizeof(char)))) {
        mlog(MPI_CRIT, "Rank %d - "
             "Error allocating space for data",
             rank_);
        return MDHIM_ERROR;
    }

    // Receive the message
    pthread_mutex_lock(&mutex_);
    return_code = MPI_Irecv(*recvbuf, *recvsize, MPI_PACKED, src, CLIENT_RESPONSE_MSG, comm_, &req);
    pthread_mutex_unlock(&mutex_);
    Flush(&req);

    // If the receive did not succeed then return the error code back
    if ( return_code != MPI_SUCCESS ) {
        mlog(MPI_CRIT, "Rank %d - "
             "Error receiving message in receive_client_response",
             rank_);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

/**
 * only_receive_all_client_responses
 * Receives messages from multiple sources sources
 *
 * @param md            in  main MDHIM struct
 * @param srcs          in  sources to receive from
 * @param nsrcs         in  number of sources to receive from
 * @param messages out  array of messages to receive
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int MPITransportBase::receive_all_client_responses(int *srcs, int nsrcs, void ***recvbufs, int **sizebuf) {
    MPI_Status status;
    int return_code;
    int mtype;
    int ret = MDHIM_SUCCESS;
    MPI_Request **reqs = new MPI_Request*[nsrcs]();
    int done = 0;

    *sizebuf = (int *)calloc(nsrcs, sizeof(int));
    *recvbufs = (void **)calloc(nsrcs, sizeof(void *));

    // Receive a size message from the servers in the list
    for (int i = 0; i < nsrcs; i++) {
        reqs[i] = new MPI_Request();
        pthread_mutex_lock(&mutex_);
        return_code = MPI_Irecv(*sizebuf + i, sizeof(**sizebuf), MPI_CHAR,
                                srcs[i], CLIENT_RESPONSE_SIZE_MSG,
                                comm_, reqs[i]);
        pthread_mutex_unlock(&mutex_);

        // If the receive did not succeed then return the error code back
        if ( return_code != MPI_SUCCESS ) {
            mlog(MPI_CRIT, "Rank %d - "
                 "Error receiving message in receive_client_response",
                 rank_);
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

            pthread_mutex_lock(&mutex_);
            return_code = MPI_Test(reqs[i], &flag, &status);
            pthread_mutex_unlock(&mutex_);

            if (return_code == MPI_ERR_REQUEST) {
                mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Received an error status: %d "
                     " while receiving client response message size", rank_, status.MPI_ERROR);
            }
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
        *recvbufs[i] = malloc((*sizebuf)[i]);
        reqs[i] = new MPI_Request();

        pthread_mutex_lock(&mutex_);
        return_code = MPI_Irecv(*recvbufs[i], *sizebuf[i], MPI_PACKED,
                                srcs[i], CLIENT_RESPONSE_MSG,
                                comm_, reqs[i]);
        pthread_mutex_unlock(&mutex_);

        // If the receive did not succeed then return the error code back
        if ( return_code != MPI_SUCCESS ) {
            mlog(MPI_CRIT, "Rank %d - "
                 "Error receiving message in receive_client_response", rank_);
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

            pthread_mutex_lock(&mutex_);
            return_code = MPI_Test(reqs[i], &flag, &status);
            pthread_mutex_unlock(&mutex_);

            if (return_code == MPI_ERR_REQUEST) {
                mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Received an error status: %d "
                     " while receiving work message size", rank_, status.MPI_ERROR);
            }
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

void MPITransportBase::Flush(MPI_Request *req) {
    if (!req) {
        return;
    }

    int flag = 0;
    MPI_Status status;

    while (!flag) {
        pthread_mutex_lock(&mutex_);
        MPI_Test(req, &flag, &status);
        pthread_mutex_unlock(&mutex_);

        if (!flag) {
            usleep(100);
        }
    }
}
