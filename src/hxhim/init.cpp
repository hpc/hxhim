#include "datastore/datastore.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/private/options.hpp"
#include "transport/transports.hpp"
#include "utils/is_range_server.hpp"

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
    if (!opts->p->client_ratio    ||
        !opts->p->server_ratio    ||
        !opts->p->datastore_count) {
        return HXHIM_ERROR;
    }

    hx->p->range_server.client_ratio = opts->p->client_ratio;
    hx->p->range_server.server_ratio = opts->p->server_ratio;

    if (is_range_server(hx->p->bootstrap.rank, opts->p->client_ratio, opts->p->server_ratio)) {
        // allocate pointers
        hx->p->datastores.resize(opts->p->datastore_count);

        // create datastores
        for(std::size_t i = 0; i < hx->p->datastores.size(); i++) {
            if (datastore::Init(hx, i, opts->p->datastore, opts->p->histogram) != DATASTORE_SUCCESS) {
                return HXHIM_ERROR;
            }
        }
    }

    hx->p->total_range_servers = opts->p->server_ratio * (hx->p->bootstrap.size / opts->p->server_ratio) + (hx->p->bootstrap.size % opts->p->server_ratio);

    hx->p->total_datastores = hx->p->total_range_servers * opts->p->datastore_count;

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

    if (opts->p->datastore_count != 1) {
        return HXHIM_ERROR;
    }

    hx->p->datastores.resize(1);

    // ignore configuration hash - everything goes into here
    hx->p->hash.name = "local";
    hx->p->hash.func = hxhim_hash_RankZero;
    hx->p->hash.args = nullptr;

    if (datastore::Init(hx, 0, opts->p->datastore, opts->p->histogram, &name) != DATASTORE_SUCCESS) {
        return HXHIM_ERROR;
    }

    hx->p->total_range_servers = 1;
    hx->p->total_datastores = 1;

    return hx->p->datastores[0]?HXHIM_SUCCESS:HXHIM_ERROR;
}

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
