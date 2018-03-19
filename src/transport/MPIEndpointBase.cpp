#include "MPIEndpointBase.hpp"
#include "MPIPacker.hpp"
#include "MPIUnpacker.hpp"
#include "MPIEndpoint.hpp"

pthread_mutex_t MPIEndpointBase::mutex_ = PTHREAD_MUTEX_INITIALIZER;

MPIEndpointBase::MPIEndpointBase(const MPI_Comm comm, volatile int &shutdown)
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

MPIEndpointBase::~MPIEndpointBase() {}

MPI_Comm MPIEndpointBase::Comm() const {
    return comm_;
}

int MPIEndpointBase::Rank() const {
    return rank_;
}

int MPIEndpointBase::Size() const {
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
int MPIEndpointBase::send_rangesrv_work(int dest, const void *buf, const int size) {
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
int MPIEndpointBase::send_all_rangesrv_work(void **messages, int *sizes, int *dests, int num_srvs) {
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
 * receive_client_response message
 * Receives a message from the given source
 *
 * @param md      in   main MDHIM struct
 * @param src     in   source to receive from
 * @param message out  double pointer for message received
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int MPIEndpointBase::receive_client_response(int src, void **recvbuf, int *recvsize) {
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
int MPIEndpointBase::receive_all_client_responses(int *srcs, int nsrcs, void ***recvbufs, int **sizebuf) {
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

/**
 * respond_to_client
 * Function called by the range server to send responses to clients
 *
 * @param transport the underlying networking functionality
 * @param dest      the destination of the message within the transport
 * @param message   the data to send to the destination
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int MPIEndpointBase::respond_to_client(Transport *transport, const TransportAddress *dest, TransportMessage *message) {
    if (!transport || !dest || !message) {
        return MDHIM_ERROR;
    }

    MPIEndpointBase *mpiepb = dynamic_cast<MPIEndpointBase *>(dynamic_cast<MPIEndpoint *>(transport->Endpoint()));
    if (!mpiepb) {
        return MDHIM_ERROR;
    }

    const MPIAddress *mpidest = dynamic_cast<const MPIAddress *>(dest);
    if (!mpidest) {
        return MDHIM_ERROR;
    }

    return send_client_response(mpiepb, mpidest->Rank(), message);
}

/**
 * listen_for_client
 * Function called by the range server listener to receive messages for processing
 *
 * @param transport the underlying networking functionality
 * @param src       where the message originated from
 * @param message   the data received
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int MPIEndpointBase::listen_for_client(Transport *transport, TransportAddress **src, TransportMessage **message) {
    if (!transport || !src || !message) {
        return MDHIM_ERROR;
    }

    MPIEndpointBase *mpiepb = dynamic_cast<MPIEndpointBase *>(dynamic_cast<MPIEndpoint *>(transport->Endpoint()));
    if (!mpiepb) {
        return MDHIM_ERROR;
    }

    int mpisrc;
    if ((receive_rangesrv_work(mpiepb, &mpisrc, message) != MDHIM_SUCCESS) ||
        !(*src = new MPIAddress(mpisrc))) {
        delete *message;
        *message = nullptr;
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

void MPIEndpointBase::Flush(MPIEndpointBase *epb, MPI_Request *req, int *flag, MPI_Status *status) {
    if (!epb || !req || !flag || !status) {
        return;
    }

    while (!*flag && !epb->shutdown_) {
        usleep(100);

        pthread_mutex_lock(&epb->mutex_);
        MPI_Test(req, flag, status);
        pthread_mutex_unlock(&epb->mutex_);
    }
}

void MPIEndpointBase::Flush(MPI_Request *req) {
    int flag = 0;
    MPI_Status status;

    Flush(this, req, &flag, &status);
}

/**
 * only_send_client_response
 * Sends a buffer to a client
 *
 * @param md      main MDHIM struct
 * @param dest    destination to send to
 * @param sendbuf double pointer to packed message
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int MPIEndpointBase::only_send_client_response(MPIEndpointBase *epb, int dest, void *sendbuf, int sizebuf) {
    if (!epb || !sendbuf) {
        return MDHIM_ERROR;
    }

    int return_code = 0;
    int mtype;
    int ret = MDHIM_SUCCESS;

    MPI_Status status;
    int flag = 0;
    MPI_Request size_req;
    MPI_Request msg_req;

    //Send the size message
    pthread_mutex_lock(&epb->mutex_);
    return_code = MPI_Isend(&sizebuf, sizeof(sizebuf), MPI_CHAR, dest, CLIENT_RESPONSE_SIZE_MSG, epb->comm_, &size_req);
    pthread_mutex_unlock(&epb->mutex_);

    if (return_code != MPI_SUCCESS) {
        mlog(MPI_CRIT, "Rank %d - "
             "Error sending client response message size in send_client_response",
             epb->rank_);
        ret = MDHIM_ERROR;
    }
    Flush(epb, &size_req, &flag, &status);

    //Send the actual message
    pthread_mutex_lock(&epb->mutex_);
    return_code = MPI_Isend(sendbuf, sizebuf, MPI_PACKED, dest, CLIENT_RESPONSE_MSG, epb->comm_, &msg_req);
    pthread_mutex_unlock(&epb->mutex_);

    if (return_code != MPI_SUCCESS) {
        mlog(MPI_CRIT, "Rank %d - "
             "Error sending client response message in send_client_response",
             epb->rank_);
        ret = MDHIM_ERROR;
    }

    Flush(epb, &msg_req, &flag, &status);

    return ret;
}

/**
 * send_client_response
 * Sends a message to a client
 *
 * @param md      main MDHIM struct
 * @param dest    destination to send to
 * @param message pointer to message to send
 * @param sendbuf double pointer to packed message
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int MPIEndpointBase::send_client_response(MPIEndpointBase *epb, int dest, TransportMessage *message) {
    int ret = MDHIM_ERROR;
    void *sendbuf = nullptr;
    int sizebuf = 0;

    if ((ret = MPIPacker::any(epb, message, &sendbuf, &sizebuf)) == MDHIM_SUCCESS) {
        ret = only_send_client_response(epb, dest, sendbuf, sizebuf);
    }

    ::operator delete(sendbuf);

    return ret;
}

/**
 * only_receive_rangesrv_work message
 * Receives a message (octet buf) from the given source
 *
 * @param md       in   main MDHIM struct
 * @param src      out  pointer to source of message received
 * @param recvbuf  out  double pointer for message received
 * @param recvsize out  pointer for the size of the message received
 * @return MDHIM_SUCCESS, MDHIM_CLOSE, MDHIM_COMMIT, or MDHIM_ERROR on error
 */
int MPIEndpointBase::only_receive_rangesrv_work(MPIEndpointBase *epb, int *src, void **recvbuf, int *recvsize) {
    if (!epb || !src || !recvbuf || !recvsize) {
        return MDHIM_ERROR;
    }

    int return_code;
    MPI_Status status;
    MPI_Request req;
    int flag = 0;
    int ret = MDHIM_SUCCESS;

    // force the srouce rank to be bad
    status.MPI_SOURCE = -1;

    // Receive a message size from any client
    pthread_mutex_lock(&epb->mutex_);
    return_code = MPI_Irecv(recvsize, sizeof(*recvsize), MPI_CHAR, MPI_ANY_SOURCE, RANGESRV_WORK_SIZE_MSG, epb->comm_, &req);
    pthread_mutex_unlock(&epb->mutex_);

    // If the receive did not succeed then return the error code back
    if ( return_code != MPI_SUCCESS ) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Error: %d "
             "receive size message failed.", epb->rank_, return_code);
        return MDHIM_ERROR;
    }
    Flush(epb, &req, &flag, &status);

    if (return_code == MPI_ERR_IN_STATUS) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Received an error status: %d "
             " while receiving work message size", epb->rank_, status.MPI_ERROR);
    }

    *recvbuf = (char *)calloc(*recvsize, sizeof(char));
    flag = 0;

    // Receive the message from the client
    pthread_mutex_lock(&epb->mutex_);
    return_code = MPI_Irecv((void *)*recvbuf, *recvsize, MPI_PACKED, status.MPI_SOURCE, RANGESRV_WORK_MSG, epb->comm_, &req);
    pthread_mutex_unlock(&epb->mutex_);

    // If the receive did not succeed then return the error code back
    if ( return_code != MPI_SUCCESS ) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Error: %d "
             "receive message failed.", epb->rank_, return_code);
        return MDHIM_ERROR;
    }
    Flush(epb, &req, &flag, &status);

    if (return_code == MPI_ERR_IN_STATUS) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Received an error status: %d "
             " while receiving work message", epb->rank_, status.MPI_ERROR);
    }
    if (!*recvbuf) {
        return MDHIM_ERROR;
    }

    *src = status.MPI_SOURCE;

    return ret;
}

/**
 * receive_rangesrv_work message
 * Receives a message from the given source
 *
 * @param md      in   main MDHIM struct
 * @param src     out  pointer to source of message received
 * @param message out  double pointer for message received
 * @return MDHIM_SUCCESS, MDHIM_CLOSE, MDHIM_COMMIT, or MDHIM_ERROR on error
 */
int MPIEndpointBase::receive_rangesrv_work(MPIEndpointBase *epb, int *src, TransportMessage **message) {
    void *recvbuf = nullptr;
    int recvsize = 0;

    int ret = MDHIM_ERROR;
    if ((ret = only_receive_rangesrv_work(epb, src, &recvbuf, &recvsize)) == MDHIM_SUCCESS) {
        ret = MPIUnpacker::any(epb, message, recvbuf, recvsize);
    }

    free(recvbuf);
    return ret;
}
