#include <cmath>

#include "datastore/datastore.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/private/options.hpp"
#include "hxhim/private/process.hpp"
#include "hxhim/RangeServer.hpp"
#include "transport/transports.hpp"

/**
 * bootstrap
 * Sets up the MPI bootstrapping information
 *
 * @param hx   the HXHIM instance
 * @param opts the HXHIM options
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::bootstrap(hxhim_t *hx, hxhim_options_t *opts) {
    mlog(HXHIM_CLIENT_INFO, "Starting MPI Bootstrap Initialization");
    if (((hx->p->bootstrap.comm = opts->p->comm)                      == MPI_COMM_NULL) ||
        (MPI_Comm_rank(hx->p->bootstrap.comm, &hx->p->bootstrap.rank) != MPI_SUCCESS)   ||
        (MPI_Comm_size(hx->p->bootstrap.comm, &hx->p->bootstrap.size) != MPI_SUCCESS))   {
        mlog(HXHIM_CLIENT_ERR, "Failed MPI Bootstrap Initialization");
        return HXHIM_ERROR;
    }

    mlog(HXHIM_CLIENT_INFO, "Completed MPI Bootstrap Intialization");

    return HXHIM_SUCCESS;
}

/**
 * running
 * Sets the state to running
 *
 * @param hx   the HXHIM instance
 * @param opts the HXHIM options
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::running(hxhim_t *hx, hxhim_options_t *) {
    hx->p->running = true;
    return HXHIM_SUCCESS;
}

/**
 * memory
 * Sets up the memory related variables
 *
 * @param hx   the HXHIM instance
 * @param opts the HXHIM options
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::memory(hxhim_t *hx, hxhim_options_t *opts) {
    mlog(HXHIM_CLIENT_INFO, "Starting Memory Initialization");
    hx->p->max_ops_per_send = opts->p->max_ops_per_send;
    mlog(HXHIM_CLIENT_INFO, "Completed Memory Initialization");
    return HXHIM_SUCCESS;
}

/**
 * datastore
 * Sets up and starts the datastore
 *
 * @param hx   the HXHIM instance
 * @param opts the HXHIM options
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::datastore(hxhim_t *hx, hxhim_options_t *opts) {
    mlog(HXHIM_CLIENT_INFO, "Starting Datastore Initialization");

    // there must be at least 1 client, server, and datastore
    if (!opts->p->client_ratio ||
        !opts->p->server_ratio) {
        return HXHIM_ERROR;
    }

    hx->p->range_server.client_ratio = opts->p->client_ratio;
    hx->p->range_server.server_ratio = opts->p->server_ratio;

    // create datastore if this rank is a server
    if (RangeServer::is_range_server(hx->p->bootstrap.rank, opts->p->client_ratio, opts->p->server_ratio)) {
        if (datastore::Init(hx, opts->p->datastore, opts->p->histogram) != DATASTORE_SUCCESS) {
            return HXHIM_ERROR;
        }
    }

    // calculate how many range servers there are
    if (hx->p->range_server.client_ratio <= hx->p->range_server.server_ratio) {
        // all ranks are servers
        hx->p->total_range_servers = hx->p->bootstrap.size;
    }
    else {
        // client > server

        // whole "buckets" get server_ratio servers per bucket
        const std::size_t whole_buckets = hx->p->bootstrap.size / hx->p->range_server.client_ratio;
        hx->p->total_range_servers = whole_buckets * hx->p->range_server.server_ratio;

        // there can be at most server_ratio servers
        const std::size_t remaining_ranks = hx->p->bootstrap.size % hx->p->range_server.client_ratio;
        hx->p->total_range_servers += std::min(remaining_ranks, hx->p->range_server.server_ratio);
    }

    mlog(HXHIM_CLIENT_INFO, "Completed Datastore Initialization");
    return HXHIM_SUCCESS;
}

/**
 * one_datastore
 * Sets up and starts one datastore instance
 *
 * @param hx   the HXHIM instance
 * @param opts the HXHIM options
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::one_datastore(hxhim_t *hx, hxhim_options_t *opts, const std::string &name) {
    if (destroy::datastore(hx) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    hx->p->range_server.client_ratio = 1;
    hx->p->range_server.server_ratio = 1;

    // ignore configuration hash - everything goes into here
    hx->p->hash.name = "local";
    hx->p->hash.func = hxhim_hash_RankZero;
    hx->p->hash.args = nullptr;

    if (datastore::Init(hx, opts->p->datastore, opts->p->histogram, &name) != DATASTORE_SUCCESS) {
        return HXHIM_ERROR;
    }

    hx->p->total_range_servers = 1;

    return hx->p->datastore?HXHIM_SUCCESS:HXHIM_ERROR;
}

#if ASYNC_PUTS
/**
 * backgroundPUT
 * The thread that runs when the number of full batches crosses the queued bputs threshold
 *
 * @param hx      the HXHIM context
 */
static void backgroundPUT(hxhim_t *hx) {
    if (!hxhim::valid(hx)) {
        return;
    }

    mlog(HXHIM_CLIENT_DBG, "Started background PUT thread");

    while (hx->p->running) {
        hxhim::PutData *head = nullptr;    // the first PUT to process

        hxhim::Unsent<hxhim::PutData> &unsent = hx->p->queues.puts;

        // hold unsent.mutex just long enough to move queued PUTs to send queue
        {
            // Wait until any of the following is true
            //    1. HXHIM is no longer running
            //    2. The number of queued PUTs passes the threshold
            //    3. The PUTs are being forced to flush
            std::unique_lock<std::mutex> lock(unsent.mutex);
                mlog(HXHIM_CLIENT_DBG, "Waiting for %zu PUTs (currently have %zu)", hx->p->async_put.max_queued, unsent.count);
                unsent.start_processing.wait(lock, [&]() -> bool { return !hx->p->running || (unsent.count >= hx->p->async_put.max_queued); });

            mlog(HXHIM_CLIENT_DBG, "Moving %zu queued PUTs into process queue", unsent.count);

            // move all PUTs into this thread for processing
            head = unsent.take_no_lock();
        }

        mlog(HXHIM_CLIENT_DBG, "Processing queued PUTs");
        {
            // process the queued PUTs
            hxhim::Results *res = hxhim::process<hxhim::PutData, Transport::Request::BPut, Transport::Response::BPut>(hx, head);

            // store the results in a buffer that FlushPuts will clean up
            {
                std::lock_guard<std::mutex> lock(hx->p->async_put.mutex);

                if (hx->p->async_put.results) {
                    hx->p->async_put.results->Append(res);
                    destruct(res);
                }
                else {
                    hx->p->async_put.results = res;
                }
            }

            unsent.done_processing.notify_all();
        }

        mlog(HXHIM_CLIENT_DBG, "Done processing queued PUTs");
    }

    mlog(HXHIM_CLIENT_DBG, "Background PUT thread stopping");
}
#endif

/**
 * async_put
 * Starts up the background thread that does asynchronous PUTs.
 *
 * @param hx   the HXHIM instance
 * @param opts the HXHIM options
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::async_put(hxhim_t *hx, hxhim_options_t *opts) {
    if (init::running(hx, opts) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    std::lock_guard<std::mutex> lock(hx->p->async_put.mutex);

    // Set number of bulk puts to queue up before sending
    hx->p->async_put.max_queued = opts->p->start_async_put_at;

    // Set up queued PUT results list
    hx->p->async_put.results = nullptr;

    // Start the background thread
    #if ASYNC_PUTS
    hx->p->async_put.thread = std::thread(backgroundPUT, hx);
    #endif

    return HXHIM_SUCCESS;
}

/**
 * hash
 * Sets up the hash algorithm and extra arguments
 *
 * @param hx             the HXHIM instance
 * @param opts           the HXHIM options
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::hash(hxhim_t *hx, hxhim_options_t *opts) {
    mlog(HXHIM_CLIENT_INFO, "Starting Hash Initalization");

    hx->p->hash.name = opts->p->hash.name;
    hx->p->hash.func = opts->p->hash.func;
    hx->p->hash.args = opts->p->hash.args;

    mlog(HXHIM_CLIENT_INFO, "Completed Hash Initalization");
    return HXHIM_SUCCESS;
}

/**
 * transport
 * Starts up the transport layer
 *
 * @param hx   the HXHIM instance
 * @param opts the HXHIM options
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::transport(hxhim_t *hx, hxhim_options_t *opts) {
    mlog(HXHIM_CLIENT_INFO, "Starting Transport Initialization");

    if (!opts->p->client_ratio) {
        mlog(HXHIM_CLIENT_ERR, "Client ratio must be at least 1");
        return HXHIM_ERROR;
    }

    if (!opts->p->server_ratio) {
        mlog(HXHIM_SERVER_ERR, "Server ratio must be at least 1");
        return HXHIM_ERROR;
    }

    destroy::transport(hx);

    const int ret = Transport::init(hx,
                                    opts->p->client_ratio,
                                    opts->p->server_ratio,
                                    opts->p->endpointgroup,
                                    opts->p->transport);

    mlog(HXHIM_CLIENT_INFO, "Completed Transport Initialization");
    return (ret == TRANSPORT_SUCCESS)?HXHIM_SUCCESS:HXHIM_ERROR;
}
