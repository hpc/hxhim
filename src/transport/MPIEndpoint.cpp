#include "MPIEndpoint.hpp"

#define HXHIM_MPI_REQUEST_TAG 0x311

MPIEndpoint::MPIEndpoint(const MPI_Comm comm, const int remote_rank, volatile int &shutdown)
    : TransportEndpoint(),
      MPIEndpointBase(comm, shutdown),
      remote_rank_(remote_rank)
{}

/**
 * Put
 * Sends a TransportPutMessage to the other end of the endpoint
 *
 * @param request the initiating PUT message
 * @return a pointer to the response of the PUT operation
 */
TransportRecvMessage *MPIEndpoint::Put(const TransportPutMessage *message) {
    return do_operation<TransportPutMessage, TransportRecvMessage>(message);
}

/**
 * Get
 * Sends a TransportGetMessage to the other end of the endpoint
 *
 * @param request the initiating GET message
 * @return a pointer to the response of the GET operation
 */
TransportGetRecvMessage *MPIEndpoint::Get(const TransportGetMessage *message) {
    TransportGetRecvMessage *grm = do_operation<TransportGetMessage, TransportGetRecvMessage>(message);
   return grm;
}

/**
 * Delete
 * Sends a TransportDeleteMessage to the other end of the endpoint
 *
 * @param request the initiating DELETE message
 * @return a pointer to the response of the DELETE operation
 */
TransportRecvMessage *MPIEndpoint::Delete(const TransportDeleteMessage *message) {
    return do_operation<TransportDeleteMessage, TransportRecvMessage>(message);
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
int MPIEndpoint::send_rangesrv_work(const void *buf, const int size) {
    int return_code = MDHIM_ERROR;
    MPI_Request req;

    //Send the size of the message
    pthread_mutex_lock(&mutex_);
    return_code = MPI_Isend(&size, sizeof(size), MPI_CHAR, remote_rank_, RANGESRV_WORK_SIZE_MSG, comm_, &req);
    pthread_mutex_unlock(&mutex_);
    Flush(&req);

    if (return_code != MPI_SUCCESS) {
        return MDHIM_ERROR;
    }

    //Send the message
    pthread_mutex_lock(&mutex_);
    return_code = MPI_Isend(buf, size, MPI_PACKED, remote_rank_, RANGESRV_WORK_MSG, comm_, &req);
    pthread_mutex_unlock(&mutex_);
    Flush(&req);

    if (return_code != MPI_SUCCESS) {
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
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
int MPIEndpoint::receive_client_response(void **buf, int *size) {
    int return_code;
    MPI_Request req;

    // Receive the size of the message
    pthread_mutex_lock(&mutex_);
    return_code = MPI_Irecv(size, sizeof(*size), MPI_CHAR, remote_rank_, CLIENT_RESPONSE_SIZE_MSG, comm_, &req);
    pthread_mutex_unlock(&mutex_);
    Flush(&req);

    // If the receive did not succeed then return the error code back
    if (return_code != MPI_SUCCESS) {
        return MDHIM_ERROR;
    }

    // allocate space for the message
    if (!(*buf = Memory::MESSAGE_BUFFER::Instance().acquire(*size))) {
        return MDHIM_ERROR;
    }

    // Receive the message
    pthread_mutex_lock(&mutex_);
    return_code = MPI_Irecv(*buf, *size, MPI_PACKED, remote_rank_, CLIENT_RESPONSE_MSG, comm_, &req);
    pthread_mutex_unlock(&mutex_);
    Flush(&req);

    // If the receive did not succeed then return the error code back
    if ( return_code != MPI_SUCCESS ) {
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}
