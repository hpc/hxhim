#include <cmath>

#include "datastore/datastore.hpp"
#include "datastore/transform.hpp"
#include "hxhim/RangeServer.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/private/options.hpp"
#include "hxhim/private/process.hpp"
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
 * queues
 * Sets up the queue related variables
 * The total number of range servers across
 * all ranks must be known by this point
 *
 * @param hx   the HXHIM instance
 * @param opts the HXHIM options
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::queues(hxhim_t *hx, hxhim_options_t *opts) {
    mlog(HXHIM_CLIENT_INFO, "Starting Memory Initialization");
    hx->p->queues.max_per_request.ops = opts->p->max_per_request.ops;
    hx->p->queues.max_per_request.size = opts->p->max_per_request.size;

    {
        #if ASYNC_PUTS
        std::lock_guard<std::mutex> lock(hx->p->queues.puts.mutex);
        #endif
        hx->p->queues.puts.queue.resize(hx->p->range_server.total_range_servers);
        hx->p->queues.puts.count = 0;
    }
    hx->p->queues.gets.resize      (hx->p->range_server.total_range_servers);
    hx->p->queues.getops.resize    (hx->p->range_server.total_range_servers);
    hx->p->queues.deletes.resize   (hx->p->range_server.total_range_servers);
    hx->p->queues.histograms.resize(hx->p->range_server.total_range_servers);
    hx->p->queues.rs_to_rank.resize(hx->p->range_server.total_range_servers);

    for(std::size_t i = 0; i < hx->p->range_server.total_range_servers; i++) {
        hx->p->queues.rs_to_rank[i] = hxhim::RangeServer::get_rank(i, hx->p->bootstrap.size,
                                                                   hx->p->range_server.client_ratio,
                                                                   hx->p->range_server.server_ratio);
    }


    mlog(HXHIM_CLIENT_INFO, "Completed Memory Initialization");
    return HXHIM_SUCCESS;
}

namespace hxhim {
namespace init {

Datastore::Transform::Callbacks *transform(hxhim_options_t *opts) {
    Datastore::Transform::Callbacks *callbacks = Datastore::Transform::default_callbacks();

    // move in values from opts
    callbacks->numeric_extra = opts->p->transform.numeric_extra;

    // overwrite existing callbacks with those set in opts
    for(decltype(opts->p->transform.encode)::value_type const &callback : opts->p->transform.encode) {
        callbacks->encode.emplace(callback);
    }
    for(decltype(opts->p->transform.decode)::value_type const &callback : opts->p->transform.decode) {
        callbacks->decode.emplace(callback);
    }

    return callbacks;
}

}
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

    hx->p->histograms.names = std::move(opts->p->histograms.names);
    hx->p->histograms.config = opts->p->histograms.config;

    // create datastore if this rank is a server
    const int rs_id = hxhim::RangeServer::get_id(hx->p->bootstrap.rank, hx->p->bootstrap.size,
                                                 hx->p->range_server.client_ratio,
                                                 hx->p->range_server.server_ratio);
    if (rs_id > -1) {
        // and if the datastore should be created
        if (Datastore::Init(hx,
                            rs_id,
                            opts->p->datastore,
                            init::transform(opts),
                            opts->p->histograms.config,
                            nullptr,
                            opts->p->open_init_datastore,
                            opts->p->histograms.read,
                            opts->p->histograms.write) != DATASTORE_SUCCESS) {
            return HXHIM_ERROR;
        }
    }

    // calculate how many range servers there are
    if (hx->p->range_server.client_ratio <= hx->p->range_server.server_ratio) {
        // all ranks are servers
        hx->p->range_server.total_range_servers = hx->p->bootstrap.size;
    }
    else {
        // client > server

        // whole "buckets" get server_ratio servers per bucket
        const std::size_t whole_buckets = hx->p->bootstrap.size / hx->p->range_server.client_ratio;
        hx->p->range_server.total_range_servers = whole_buckets * hx->p->range_server.server_ratio;

        // there can be at most server_ratio servers
        const std::size_t remaining_ranks = hx->p->bootstrap.size % hx->p->range_server.client_ratio;
        hx->p->range_server.total_range_servers += std::min(remaining_ranks, hx->p->range_server.server_ratio);
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
 * @param name the name of the datastore, not including the prefix
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::one_datastore(hxhim_t *hx, hxhim_options_t *opts, const std::string &name) {
    if (destroy::datastore(hx) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    hx->p->range_server.client_ratio = 1;
    hx->p->range_server.server_ratio = 1;

    hx->p->histograms.names = std::move(opts->p->histograms.names);

    // ignore configuration hash - everything goes into here
    hx->p->hash.name = "local";
    hx->p->hash.func = hxhim_hash_RankZero;
    hx->p->hash.args = nullptr;

    if (Datastore::Init(hx, 0,
                        opts->p->datastore,
                        init::transform(opts),
                        opts->p->histograms.config,
                        &name,
                        opts->p->open_init_datastore,
                        opts->p->histograms.read,
                        opts->p->histograms.write) != DATASTORE_SUCCESS) {
        return HXHIM_ERROR;
    }

    hx->p->range_server.total_range_servers = 1;

    return hx->p->range_server.datastore?HXHIM_SUCCESS:HXHIM_ERROR;
}

#if ASYNC_PUTS
/**
 * backgroundPUT
 * The thread that runs when the number of full batches crosses the queued bputs threshold
 *
 * @param hx      the HXHIM context
 */
static void backgroundPUT(hxhim_t *hx) {
    // if (!hxhim::valid(hx)) {
    //     return;
    // }

    mlog(HXHIM_CLIENT_DBG, "Started background PUT thread");

    while (hx->p->running) {
        hxhim::Queues<Transport::Request::BPut> puts;
        {
            // wait for number of PUTs to reach limit
            // PUTs/FlushPuts triggers check
            std::unique_lock<std::mutex> queue_lock(hx->p->queues.puts.mutex);
            hx->p->queues.puts.start_processing.wait(queue_lock,
                                                     [hx]() -> bool {
                                                         return (!hx->p->running ||
                                                                 (hx->p->queues.puts.count >= hx->p->async_put.max_queued) ||
                                                                 hx->p->queues.puts.flushed);
                                                     }
                );

            // puts.mutex now locked

            // take PUTs and restore queue
            puts = std::move(hx->p->queues.puts.queue);
            hx->p->queues.puts.queue.clear();
            hx->p->queues.puts.queue.resize(hx->p->range_server.total_range_servers);
            hx->p->queues.puts.count = 0;
            hx->p->queues.puts.flushed = false;
        }

        // puts.mutex unlocked
        // new PUTs can enter queue while processing occurs

        // process PUTs
        hxhim::Results *results = hxhim::process<Transport::Request::BPut, Transport::Response::BPut>(hx, puts);
        {
            std::lock_guard<std::mutex> async_lock(hx->p->async_put.mutex);

            // store/append results
            if (hx->p->async_put.results) {
                hx->p->async_put.results->Append(results);
                destruct(results);
            }
            else {
                hx->p->async_put.results = results;
            }

            hx->p->async_put.done_check = true;
        }

        hx->p->async_put.done.notify_all();
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

    // Set number of bulk puts to queue up before sending
    hx->p->async_put.max_queued = opts->p->start_async_put_at;

    // Set up queued PUT results list
    hx->p->async_put.results = nullptr;

    #if ASYNC_PUTS
    hx->p->queues.puts.flushed = true;
    hx->p->async_put.done_check = false;

    // Start the background thread
    hx->p->async_put.thread = std::thread(backgroundPUT, hx);
    hxhim::wait_for_background_puts(hx);
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
