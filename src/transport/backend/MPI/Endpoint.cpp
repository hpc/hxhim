#include "transport/backend/MPI/Endpoint.hpp"
#include "transport/backend/MPI/constants.h"

namespace Transport {
namespace MPI {

Endpoint::Endpoint(const MPI_Comm comm,
                   const int remote_rank,
                   volatile std::atomic_bool &running,
                   std::shared_ptr<FixedBufferPool> packed,
                   FixedBufferPool *responses,
                   FixedBufferPool *arrays,
                   FixedBufferPool *buffers)
  : ::Transport::Endpoint(),
    EndpointBase(comm, packed),
    remote_rank(remote_rank),
    running(running),
    responses(responses),
    arrays(arrays),
    buffers(buffers)
{}

/**
 * Put
 * Sends a Request::Put to the other end of the endpoint
 *
 * @param request the initiating PUT message
 * @return a pointer to the response of the PUT operation
 */
Response::Put *Endpoint::Put(const Request::Put *message) {
    return do_operation<Request::Put, Response::Put>(message);
}

/**
 * Get
 * Sends a Request::Get to the other end of the endpoint
 *
 * @param request the initiating GET message
 * @return a pointer to the response of the GET operation
 */
Response::Get *Endpoint::Get(const Request::Get *message) {
    return do_operation<Request::Get, Response::Get>(message);
}

/**
 * Delete
 * Sends a Request::Delete to the other end of the endpoint
 *
 * @param request the initiating DELETE message
 * @return a pointer to the response of the DELETE operation
 */
Response::Delete *Endpoint::Delete(const Request::Delete *message) {
    return do_operation<Request::Delete, Response::Delete>(message);
}

/**
 * Histogram
 * Sends a Request::Histogram to the other end of the endpoint
 *
 * @param request the initiating HISTOGRAM message
 * @return a pointer to the response of the HISTOGRAM operation
 */
Response::Histogram *Endpoint::Histogram(const Request::Histogram *message) {
    return do_operation<Request::Histogram, Response::Histogram>(message);
}

int Endpoint::send(void *data, const std::size_t len) {
    MPI_Request request;

    // send the size of the data
    if ((MPI_Send(&len, sizeof(len), MPI_CHAR, remote_rank, TRANSPORT_MPI_SIZE_REQUEST_TAG, comm) == MPI_SUCCESS) ||
        (Flush(request) != TRANSPORT_SUCCESS)) {
        return TRANSPORT_ERROR;
    }

    // wait for the data
    if ((MPI_Send(data, len, MPI_CHAR, remote_rank, TRANSPORT_MPI_DATA_REQUEST_TAG, comm) == MPI_SUCCESS) ||
        (Flush(request) != TRANSPORT_SUCCESS)) {
        return TRANSPORT_ERROR;
    }

    return TRANSPORT_SUCCESS;
}

int Endpoint::recv(void **data, std::size_t *len) {
    if (!data || !len) {
        return TRANSPORT_ERROR;
    }

    MPI_Request request;

    // wait for the size of the data
    if ((MPI_Irecv(len, sizeof(*len), MPI_CHAR, remote_rank, TRANSPORT_MPI_SIZE_RESPONSE_TAG, comm, &request) != MPI_SUCCESS) ||
        (Flush(request) != TRANSPORT_SUCCESS)) {
        return TRANSPORT_ERROR;
    }

    *data = packed->acquire(*len);

    // wait for the data
    if ((MPI_Recv(*data, *len, MPI_CHAR, remote_rank, TRANSPORT_MPI_DATA_RESPONSE_TAG, comm, MPI_STATUS_IGNORE) == MPI_SUCCESS) ||
        (Flush(request) != TRANSPORT_SUCCESS)) {
        return TRANSPORT_ERROR;
    }

    return TRANSPORT_SUCCESS;
}

int Endpoint::Flush(MPI_Request &req) {
    int flag = 0;
    MPI_Status status;

    while (!flag && running) {
        MPI_Test(&req, &flag, &status);
    }

    if (flag) {
        return TRANSPORT_SUCCESS;
    }

    MPI_Request_free(&req);
    return TRANSPORT_ERROR;
}

}
}
