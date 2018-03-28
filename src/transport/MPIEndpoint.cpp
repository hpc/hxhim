#include <iostream>

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
    TransportRecvMessage *reply = nullptr;
    if (PutRequest(message) == MDHIM_SUCCESS) {
        PutReply(&reply);
    }

    return reply;
}

/**
 * Get
 * Sends a TransportGetMessage to the other end of the endpoint
 *
 * @param request the initiating GET message
 * @return a pointer to the response of the GET operation
 */
TransportGetRecvMessage *MPIEndpoint::Get(const TransportGetMessage *message) {
    TransportGetRecvMessage *reply = nullptr;
    if (GetRequest(message) == MDHIM_SUCCESS) {
        GetReply(&reply);
    }

    return reply;
}

/**
 * PutRequest
 * Sends a TransportPutMessage to the other end of the endpoint
 * without waiting for an acknowledgment
 *
 * @param message the initiating PUT message
 * @return MDHIM_SUCCESS or MDHIM_ERROR
 */
int MPIEndpoint::PutRequest(const TransportPutMessage *message) {
    if (!message) {
        return MDHIM_ERROR;
    }

    // pack the message
    void *buf = nullptr;
    int size = 0;
    if (MPIPacker::pack(comm_, message, &buf, &size) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    // send the message
    const int ret = send_rangesrv_work(buf, size);

    ::operator delete(buf);

    return ret;
}

/**
 * PutReply
 * Waits for a PUT acknowledgment from the range server
 *
 * @param message the buffer that will be used to store the reply
 * @return MDHIM_SUCCESS or MDHIM_ERROR
 */
int MPIEndpoint::PutReply(TransportRecvMessage **message) {
    if (!message) {
        return MDHIM_ERROR;
    }

    // receive the message
    void *buf = nullptr;
    int size = 0;
    int ret = MDHIM_ERROR;
    if ((ret = receive_client_response(&buf, &size)) == MDHIM_SUCCESS) {
        // unpack the message
        ret = MPIUnpacker::unpack(comm_, message, buf, size);
    }

    ::operator delete(buf);

    return ret;
}

/**
 * GetRequest
 * Sends a TransportGetMessage to the other end of the endpoint
 * without waiting for an acknowledgment
 *
 * @param message the initiating GET message
 * @return MDHIM_SUCCESS or MDHIM_ERROR
 */
int MPIEndpoint::GetRequest(const TransportGetMessage *message) {
    if (!message) {
        return MDHIM_ERROR;
    }

    // pack the message
    void *buf = nullptr;
    int size = 0;
    if (MPIPacker::pack(comm_, message, &buf, &size) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    // send the message
    const int ret = send_rangesrv_work(buf, size);

    ::operator delete(buf);

    return ret;
}

/**
 * GetReply
 * Waits for a GET acknowledgment from the range server
 *
 * @param message the buffer that will be used to store the reply
 * @return MDHIM_SUCCESS or MDHIM_ERROR
 */
int MPIEndpoint::GetReply(TransportGetRecvMessage **message) {
    if (!message) {
        return MDHIM_ERROR;
    }

    // receive the message
    void *buf = nullptr;
    int size = 0;
    int ret = MDHIM_ERROR;
    if ((ret = receive_client_response(&buf, &size)) == MDHIM_SUCCESS) {
        // unpack the message
        ret = MPIUnpacker::unpack(comm_, message, buf, size);
    }

    ::operator delete(buf);

    return ret;
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
    if (!(*buf = ::operator new(*size))) {
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
