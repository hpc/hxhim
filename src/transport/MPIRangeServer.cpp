#include "MPIRangeServer.hpp"

pthread_mutex_t MPIRangeServer::mutex_ = PTHREAD_MUTEX_INITIALIZER;

/*
 * listener_thread
 * Function for the thread that listens for new messages
 */
void *MPIRangeServer::listener_thread(void *data) {
    //Mlog statements could cause a deadlock on range_server_stop due to canceling of threads
    mdhim_t *md = (mdhim_t *) data;

    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

    while (!md->p->shutdown) {
        //Clean outstanding sends
        range_server_clean_oreqs(md);

        //Receive messages sent to this server
        TransportMessage *message = nullptr;
        if (receive_rangesrv_work(&message, md->p->shutdown) != MDHIM_SUCCESS) {
            continue;
        }

        //Create a new work item
        work_item_t *item = Memory::FBP_MEDIUM::Instance().acquire<work_item_t>();

        //Set the new buffer to the new item's message
        item->message = message;
        //Set the source in the work item
        item->address = message->src;
        //Add the new item to the work queue
        range_server_add_work(md, item);
    }

    return NULL;
}

void MPIRangeServer::Flush(MPI_Request *req, int *flag, MPI_Status *status, volatile int &shutdown) {
    if (!req || !flag || !status) {
        return;
    }

    while (!*flag && !shutdown) {
        usleep(100);

        pthread_mutex_lock(&mutex_);
        MPI_Test(req, flag, status);
        pthread_mutex_unlock(&mutex_);
    }
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
int MPIRangeServer::only_send_client_response(int dest, void *sendbuf, int sizebuf, volatile int &shutdown) {
    if (!sendbuf) {
        return MDHIM_ERROR;
    }

    int return_code = 0;
    int ret = MDHIM_SUCCESS;

    MPI_Status status;
    int flag = 0;
    MPI_Request size_req;
    MPI_Request msg_req;

    //Send the size message
    pthread_mutex_lock(&mutex_);
    return_code = MPI_Isend(&sizebuf, sizeof(sizebuf), MPI_CHAR, dest, CLIENT_RESPONSE_SIZE_MSG, MPI_COMM_WORLD, &size_req);
    pthread_mutex_unlock(&mutex_);

    if (return_code != MPI_SUCCESS) {
        ret = MDHIM_ERROR;
    }
    Flush(&size_req, &flag, &status, shutdown);

    //Send the actual message
    pthread_mutex_lock(&mutex_);
    return_code = MPI_Isend(sendbuf, sizebuf, MPI_PACKED, dest, CLIENT_RESPONSE_MSG, MPI_COMM_WORLD, &msg_req);
    pthread_mutex_unlock(&mutex_);

    if (return_code != MPI_SUCCESS) {
        ret = MDHIM_ERROR;
    }

    Flush(&msg_req, &flag, &status, shutdown);

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
int MPIRangeServer::send_client_response(work_item_t *item, TransportMessage *message, volatile int &shutdown) {
    int ret = MDHIM_ERROR;
    void *sendbuf = nullptr;
    int sizebuf = 0;

    if ((ret = MPIPacker::any(MPI_COMM_WORLD, message, &sendbuf, &sizebuf)) == MDHIM_SUCCESS) {
        ret = only_send_client_response(item->address, sendbuf, sizebuf, shutdown);
    }

    Memory::FBP_MEDIUM::Instance().release(sendbuf);

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
int MPIRangeServer::only_receive_rangesrv_work(void **recvbuf, int *recvsize, volatile int &shutdown) {
    if (!recvbuf || !recvsize) {
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
    pthread_mutex_lock(&mutex_);
    return_code = MPI_Irecv(recvsize, sizeof(*recvsize), MPI_CHAR, MPI_ANY_SOURCE, RANGESRV_WORK_SIZE_MSG, MPI_COMM_WORLD, &req);
    pthread_mutex_unlock(&mutex_);

    // If the receive did not succeed then return the error code back
    if ( return_code != MPI_SUCCESS ) {
        return MDHIM_ERROR;
    }
    Flush(&req, &flag, &status, shutdown);

    *recvbuf = Memory::FBP_MEDIUM::Instance().acquire(*recvsize);
    flag = 0;

    // Receive the message from the client
    pthread_mutex_lock(&mutex_);
    return_code = MPI_Irecv(*recvbuf, *recvsize, MPI_PACKED, status.MPI_SOURCE, RANGESRV_WORK_MSG, MPI_COMM_WORLD, &req);
    pthread_mutex_unlock(&mutex_);

    // If the receive did not succeed then return the error code back
    if ( return_code != MPI_SUCCESS ) {
        return MDHIM_ERROR;
    }
    Flush(&req, &flag, &status, shutdown);

    if (!*recvbuf) {
        return MDHIM_ERROR;
    }

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
int MPIRangeServer::receive_rangesrv_work(TransportMessage **message, volatile int &shutdown) {
    void *recvbuf = nullptr;
    int recvsize = 0;

    int ret = MDHIM_ERROR;
    if ((ret = only_receive_rangesrv_work(&recvbuf, &recvsize, shutdown)) == MDHIM_SUCCESS) {
        ret = MPIUnpacker::any(MPI_COMM_WORLD, message, recvbuf, recvsize);
    }

    Memory::FBP_MEDIUM::Instance().release(recvbuf);

    return ret;
}
