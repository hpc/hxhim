#include "hxhim/hxhim.hpp"
#include "hxhim/private/hxhim.hpp"
#include "transport/backend/MPI/RangeServer.hpp"
#include "transport/backend/MPI/constants.h"
#include "transport/backend/local/RangeServer.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

namespace Transport {
namespace MPI {

RangeServer::RangeServer(hxhim_t *hx, const std::size_t listener_count)
    : hx(hx),
      listeners(listener_count)
{
    mlog(MPI_INFO, "Started MPI Range Server Initialization");
    mlog(MPI_DBG, "Starting up %zu listeners", listener_count);

    //Initialize listener threads
    for(std::size_t i = 0; i < listener_count; i++) {
        listeners[i] = std::thread(&RangeServer::listener_thread, this);
        mlog(MPI_DBG, "MPI Range Server Thread %lu Started", i);
    }

    mlog(MPI_INFO, "Completed MPI Range Server Initialization");
}

RangeServer::~RangeServer() {
    mlog(MPI_INFO, "Stopping MPI Range Server");
    mlog(MPI_DBG, "Waiting for MPI Range Server threads");
    for(std::size_t i = 0; i < listeners.size(); i++) {
        listeners[i].join();
        mlog(MPI_DBG, "MPI Range Server Thread %lu Stopped", i);
    }
    mlog(MPI_INFO, "MPI Range Server stopped");
}

/*
 * listener_thread
 * Function for the thread that listens for new messages
 */
void RangeServer::listener_thread() {
    //Mlog statements could cause a deadlock on range_server_stop due to canceling of threads
    mlog(MPI_INFO, "MPI Range Server Thread Started");
    while (hx->p->running){
        // wait for request
        void *req = nullptr;
        std::size_t len = 0;
        if (recv(&req, &len) != TRANSPORT_SUCCESS) {
            continue;
        }

        // decode request
        Message::Request::Request *request = nullptr;
        Message::Unpacker::unpack(&request, req, len);
        dealloc(req);

        // process request
        Message::Response::Response *response = local::range_server(hx, request);
        dealloc(request);

        // encode result
        void *res = nullptr;
        len = 0;
        Message::Packer::pack(response, &res, &len);

        // send result
        const int ret = send(response->dst, res, len);
        dealloc(response);
        dealloc(res);

        if (ret != TRANSPORT_SUCCESS) {
            continue;
        }
    }
    mlog(MPI_INFO, "MPI Range Server Thread Stopped");
}

/**
 * recv
 * Waits for a length from any source and
 * then waits for data from the same source
 * Although MPI_Irecv is used, it is blocked
 * on immediately after.
 *
 * @param data the data to receive
 * @param len  the length of the data
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
 */
int RangeServer::recv(void **data, std::size_t *len) {
    if (!data || !len) {
        return TRANSPORT_ERROR;
    }

    MPI_Request request = {};
    MPI_Status status = {};

    // wait for the size of the data
    mlog(MPI_DBG, "MPI Range Server waiting for size");
    if ((MPI_Irecv(len, sizeof(*len), MPI_CHAR, MPI_ANY_SOURCE, TRANSPORT_MPI_SIZE_REQUEST_TAG, hx->p->bootstrap.comm, &request) != MPI_SUCCESS) ||
        (Flush(request, status) != TRANSPORT_SUCCESS)) {
        mlog(MPI_DBG, "MPI Range Server errored while waiting for size");
        return TRANSPORT_ERROR;
    }
    mlog(MPI_DBG, "MPI Range Server got size %zu", *len);

    *data = alloc(*len);

    // wait for the data
    // mlog(MPI_DBG, "MPI Range Server waiting for data");
    if ((MPI_Irecv(*data, *len, MPI_CHAR, status.MPI_SOURCE, TRANSPORT_MPI_DATA_REQUEST_TAG, hx->p->bootstrap.comm, &request) != MPI_SUCCESS) ||
        (Flush(request) != TRANSPORT_SUCCESS)) {
        // mlog(MPI_ERR, "MPI_Range Server errored while getting data of size %zu", *len);
        return TRANSPORT_ERROR;
    }

    // mlog(MPI_DBG, "MPI Range Server got data of size %zu", *len);
    return TRANSPORT_SUCCESS;
}

/**
 * send
 * Sends a length followed by the data to the
 * given destination. Although MPI_Isend is
 * used, it is blocked on immediately after.
 *
 * @param dst  the destination of the data
 * @param data the data to receive
 * @param len  the length of the data
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
 */
int RangeServer::send(const int dst, void *data, const std::size_t len) {
    MPI_Request request = {};

    // send the size of the data
    // mlog(MPI_DBG, "MPI Range Server sending size %zu", len);
    if ((MPI_Isend(&len, sizeof(len), MPI_CHAR, dst, TRANSPORT_MPI_SIZE_RESPONSE_TAG, hx->p->bootstrap.comm, &request) != MPI_SUCCESS) ||
        (Flush(request) != TRANSPORT_SUCCESS)) {
        return TRANSPORT_ERROR;
    }

    // wait for the data
    // mlog(MPI_DBG, "MPI Range Server sending data");
    if ((MPI_Isend(data, len, MPI_CHAR, dst, TRANSPORT_MPI_DATA_RESPONSE_TAG, hx->p->bootstrap.comm, &request) != MPI_SUCCESS) ||
        (Flush(request) != TRANSPORT_SUCCESS)) {
        return TRANSPORT_ERROR;
    }

    // mlog(MPI_DBG, "MPI Range Server send done");
    return TRANSPORT_SUCCESS;
}

/**
 * Flush
 * Tests a request until it is completed.
 * This function can exit early if the
 * HXHIM instance stops first.
 *
 * @param req the request to test
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
 */
int RangeServer::Flush(MPI_Request &req) {
    int flag = 0;
    while (!flag && hx->p->running) {
        MPI_Test(&req, &flag, MPI_STATUS_IGNORE);
    }

    if (flag) {
        // mlog(MPI_DBG, "MPI Range Server flush succeeded");
        return TRANSPORT_SUCCESS;
    }

    // mlog(MPI_DBG, "MPI Range Server flush failed (flag %d, running %d)", flag, hx->p->running.load());
    MPI_Request_free(&req);
    return TRANSPORT_ERROR;
}

 /**
 * Flush
 * Tests a request until it is completed.
 * This function can exit early if the
 * HXHIM instance stops first.
 *
 * @param req    the request to test
 * @param status the status of the request
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
 */
int RangeServer::Flush(MPI_Request &req, MPI_Status &status) {
    int flag = 0;
    while (!flag && hx->p->running) {
        MPI_Test(&req, &flag, &status);
    }

    if (flag) {
        // mlog(MPI_DBG, "MPI Range Server flush succeeded");
        return TRANSPORT_SUCCESS;
    }

    // mlog(MPI_DBG, "MPI Range Server flush failed (flag %d, running %d)", flag, hx->p->running.load());
    MPI_Request_free(&req);
    return TRANSPORT_ERROR;
}

}
}
