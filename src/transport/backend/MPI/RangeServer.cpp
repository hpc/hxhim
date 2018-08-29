#include "hxhim/private.hpp"
#include "hxhim/range_server.hpp"
#include "hxhim/struct.h"
#include "transport/backend/MPI/Packer.hpp"
#include "transport/backend/MPI/RangeServer.hpp"
#include "transport/backend/MPI/Unpacker.hpp"
#include "transport/backend/MPI/constants.h"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

namespace Transport {
namespace MPI {

hxhim_t *RangeServer::hx_ = nullptr;
std::atomic_bool *RangeServer::running_ = nullptr;
std::vector<pthread_t> RangeServer::listeners_ = {};
pthread_mutex_t RangeServer::mutex_ = PTHREAD_MUTEX_INITIALIZER;
FixedBufferPool *RangeServer::packed_ = nullptr;
FixedBufferPool *RangeServer::arrays_ = nullptr;
FixedBufferPool *RangeServer::buffers_ = nullptr;

int RangeServer::init(hxhim_t *hx, const std::size_t listener_count) {
    if (!hx || !listener_count) {
        return TRANSPORT_ERROR;
    }

    hx_ = hx;
    listeners_.resize(listener_count);

    //Initialize listener threads
    for(pthread_t & pid : listeners_) {
        if (pthread_create(&pid, nullptr,
                           listener_thread, nullptr) != 0) {
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

void RangeServer::destroy() {
    for(pthread_t & pid : listeners_) {
        pthread_join(pid, nullptr);
    }

    hx_ = nullptr;
}

/*
 * listener_thread
 * Function for the thread that listens for new messages
 */
void *RangeServer::listener_thread(void *data) {
    //Mlog statements could cause a deadlock on range_server_stop due to canceling of threads
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, nullptr);
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, nullptr);

    while (hx_->p->running){
        // wait for request
        void *data = nullptr;
        std::size_t len = 0;
        if (recv(&data, &len) != TRANSPORT_SUCCESS) {
            continue;
        }

        // decode request
        Request::Request *request = nullptr;
        Unpacker::unpack(hx_->p->bootstrap.comm, &request, data, len, hx_->p->memory_pools.requests, hx_->p->memory_pools.arrays, hx_->p->memory_pools.buffers);
        ::operator delete(data);
        data = nullptr;
        len = 0;

        // process request
        Response::Response *response = hxhim::range_server::range_server(hx_, request);

        // encode result
        Packer::pack(hx_->p->bootstrap.comm, response, &data, &len, hx_->p->memory_pools.packed);

        // send result
        if (send(response->dst, data, len) != TRANSPORT_SUCCESS) {
            continue;
        }
    }

    return nullptr;
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

    MPI_Request request;
    MPI_Status status;

    // wait for the size of the data
    mlog(MPI_DBG, "MPI Range Server waiting for size");
    if ((MPI_Irecv(len, sizeof(*len), MPI_CHAR, MPI_ANY_SOURCE, TRANSPORT_MPI_SIZE_REQUEST_TAG, hx_->p->bootstrap.comm, &request) != MPI_SUCCESS) ||
        (Flush(request, status) != TRANSPORT_SUCCESS)) {
        mlog(MPI_DBG, "MPI Range Server errored while waiting for size");
        return TRANSPORT_ERROR;
    }
    mlog(MPI_DBG, "MPI Range Server got size %zu", *len);

    *data = ::operator new(*len);

    // wait for the data
    mlog(MPI_DBG, "MPI Range Server waiting for data");
    if ((MPI_Irecv(*data, *len, MPI_CHAR, status.MPI_SOURCE, TRANSPORT_MPI_DATA_REQUEST_TAG, hx_->p->bootstrap.comm, &request) != MPI_SUCCESS) ||
        (Flush(request) != TRANSPORT_SUCCESS)) {
        mlog(MPI_ERR, "MPI_Range Server errored while getting data of size %zu", *len);
        return TRANSPORT_ERROR;
    }

    mlog(MPI_DBG, "MPI Range Server got data of size %zu", *len);

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
    MPI_Request request;

    // send the size of the data
    mlog(MPI_DBG, "MPI Range Server sending size %zu", len);
    if ((MPI_Isend(&len, sizeof(len), MPI_CHAR, dst, TRANSPORT_MPI_SIZE_RESPONSE_TAG, hx_->p->bootstrap.comm, &request) != MPI_SUCCESS) ||
        (Flush(request) != TRANSPORT_SUCCESS)) {
        return TRANSPORT_ERROR;
    }

    // wait for the data
    mlog(MPI_DBG, "MPI Range Server sending data");
    if ((MPI_Isend(data, len, MPI_CHAR, dst, TRANSPORT_MPI_DATA_RESPONSE_TAG, hx_->p->bootstrap.comm, &request) != MPI_SUCCESS) ||
        (Flush(request) != TRANSPORT_SUCCESS)) {
        return TRANSPORT_ERROR;
    }

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
    while (!flag && hx_->p->running) {
        MPI_Test(&req, &flag, MPI_STATUS_IGNORE);
    }

    if (flag) {
        mlog(MPI_DBG, "MPI Range Server flush succeeded");
        return TRANSPORT_SUCCESS;
    }

    mlog(MPI_DBG, "MPI Range Server flush failed (flag %d, running %d)", flag, hx_->p->running.load());
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
    while (!flag && hx_->p->running) {
        MPI_Test(&req, &flag, &status);
    }

    if (flag) {
        mlog(MPI_DBG, "MPI Range Server flush succeeded");
        return TRANSPORT_SUCCESS;
    }

    mlog(MPI_DBG, "MPI Range Server flush failed (flag %d, running %d)", flag, hx_->p->running.load());
    return TRANSPORT_ERROR;
}

}
}
