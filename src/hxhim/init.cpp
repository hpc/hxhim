#include <cmath>

#include "datastore/datastores.hpp"
#include "datastore/transform.hpp"
#include "hxhim/Datastore.hpp"
#include "hxhim/RangeServer.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/private/process.hpp"
#include "transport/transports.hpp"

/**
 * bootstrap
 * Sets up the MPI bootstrapping information
 *
 * @param hx   the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::bootstrap(hxhim_t *hx) {
    mlog(HXHIM_CLIENT_INFO, "Starting MPI Bootstrap Initialization");
    if ((hx->p->bootstrap.comm == MPI_COMM_NULL)                                      ||
        (MPI_Comm_rank(hx->p->bootstrap.comm, &hx->p->bootstrap.rank) != MPI_SUCCESS) ||
        (MPI_Comm_size(hx->p->bootstrap.comm, &hx->p->bootstrap.size) != MPI_SUCCESS)) {
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
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::running(hxhim_t *hx) {
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
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::queues(hxhim_t *hx) {
    mlog(HXHIM_CLIENT_INFO, "Starting Memory Initialization");

    {
        #if ASYNC_PUTS
        std::lock_guard<std::mutex> lock(hx->p->queues.puts.mutex);
        #endif
        hx->p->queues.puts.queue.resize(hx->p->range_server.datastores.total);
        hx->p->queues.puts.count = 0;
    }
    hx->p->queues.gets.resize      (hx->p->range_server.datastores.total);
    hx->p->queues.getops.resize    (hx->p->range_server.datastores.total);
    hx->p->queues.deletes.resize   (hx->p->range_server.datastores.total);
    hx->p->queues.histograms.resize(hx->p->range_server.datastores.total);
    hx->p->queues.ds_to_rank.resize(hx->p->range_server.datastores.total);

    // amortize cost of computing rank from datastore
    for(std::size_t i = 0; i < hx->p->range_server.datastores.total; i++) {
        hx->p->queues.ds_to_rank[i] = hxhim::Datastore::get_rank(i, hx->p->bootstrap.size,
                                                                 hx->p->range_server.client_ratio,
                                                                 hx->p->range_server.server_ratio,
                                                                 hx->p->range_server.datastores.per_server);
    }


    mlog(HXHIM_CLIENT_INFO, "Completed Memory Initialization");
    return HXHIM_SUCCESS;
}

namespace hxhim {
namespace init {
static ::Datastore::Transform::Callbacks *transform(hxhim_t *hx) {
    decltype(hx->p->range_server.datastores.transform) &transform = hx->p->range_server.datastores.transform;
    ::Datastore::Transform::Callbacks *callbacks = ::Datastore::Transform::default_callbacks();

    callbacks->numeric_extra = transform.numeric_extra;

    // overwrite existing callbacks with those set in opts
    for(decltype(transform.encode)::value_type const &callback : transform.encode) {
        callbacks->encode.emplace(callback);
    }
    for(decltype(transform.decode)::value_type const &callback : transform.decode) {
        callbacks->decode.emplace(callback);
    }

    return callbacks;
}
}
}

/**
 * datastore
 * Sets up and starts the datastores
 *
 * @param hx   the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::datastore(hxhim_t *hx) {
    mlog(HXHIM_CLIENT_INFO, "Starting Datastore Initialization");
    // there must be at least 1 client, server, and datastore
    if (!hx->p->range_server.client_ratio ||
        !hx->p->range_server.server_ratio ||
        !hx->p->range_server.datastores.per_server) {
        return HXHIM_ERROR;
    }

    decltype(hx->p->range_server) *rs = &hx->p->range_server;

    // calculate how many range servers there are
    if (rs->client_ratio <= rs->server_ratio) {
        // all ranks are servers
        rs->total_range_servers = hx->p->bootstrap.size;
    }
    else {
        // client > server

        // whole "blocks" get server_ratio servers per block
        const std::size_t whole_blocks = hx->p->bootstrap.size / rs->client_ratio;
        rs->total_range_servers = rs->server_ratio * whole_blocks;

        // there can be at most server_ratio servers
        const std::size_t remaining_ranks = hx->p->bootstrap.size % rs->client_ratio;
        rs->total_range_servers += std::min(remaining_ranks, rs->server_ratio);
    }

    // each range server has the same number of datastores
    rs->datastores.total = rs->total_range_servers * rs->datastores.per_server;

    // get this rank's range server id, if it is a range server
    rs->id = hxhim::RangeServer::get_id(hx->p->bootstrap.rank, hx->p->bootstrap.size,
                                        rs->client_ratio,
                                        rs->server_ratio);
    if (rs->id < 0) {
        return HXHIM_SUCCESS;
    }

    // set up new datastores
    int ds_id = hxhim::Datastore::get_id(hx->p->bootstrap.rank, 0, hx->p->bootstrap.size,
                                         rs->client_ratio,
                                         rs->server_ratio,
                                         rs->datastores.per_server);

    rs->datastores.ds.resize(rs->datastores.per_server, nullptr);
    for(::Datastore::Datastore *&ds : hx->p->range_server.datastores.ds) {
        ds = ::Datastore::Init(hx,
                               ds_id++,
                               hx->p->range_server.datastores.config,
                               init::transform(hx),
                               hx->p->histograms.config,
                               nullptr,
                               hx->p->range_server.datastores.open_init,
                               hx->p->histograms.read,
                               hx->p->histograms.write);

        if (!ds) {
            return HXHIM_ERROR;
        }
    }

    mlog(HXHIM_CLIENT_INFO, "Completed Datastore Initialization");
    return HXHIM_SUCCESS;
}

/**
 * one_datastore
 * Sets up and starts one datastore instance
 *
 * @param hx   the HXHIM instance
 * @param name the name of the datastore, not including the prefix
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::one_datastore(hxhim_t *hx, const std::string &name) {
    if (destroy::datastore(hx) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    // ignore previous values
    hx->p->range_server.client_ratio = 1;
    hx->p->range_server.server_ratio = 1;

    // ignore configuration hash - everything goes into here
    hx->p->hash.name = "local";
    hx->p->hash.func = hxhim_hash_DatastoreZero;
    hx->p->hash.args = nullptr;

    ::Datastore::Datastore *ds = ::Datastore::Init(hx, 0,
                                                   hx->p->range_server.datastores.config,
                                                   init::transform(hx),
                                                   hx->p->histograms.config,
                                                   &name,
                                                   hx->p->range_server.datastores.open_init,
                                                   hx->p->histograms.read,
                                                   hx->p->histograms.write);
    if (!ds) {
        return HXHIM_ERROR;
    }

    hx->p->range_server.id = 0;
    hx->p->range_server.datastores.ds.push_back(ds);
    hx->p->range_server.total_range_servers = 1;

    return HXHIM_SUCCESS;
}

/**
 * async_put_thread
 * The thread that runs when the number of full batches crosses the queued bputs threshold
 *
 * @param hx      the HXHIM context
 */
static void async_put_thread(hxhim_t *hx) {
    // if (!hxhim::valid(hx)) {
    //     return;
    // }

    mlog(HXHIM_CLIENT_DBG, "Started background PUT thread");

    while (hx->p->running) {
        hxhim::Queues<Message::Request::BPut> puts;
        {
            // wait for number of PUTs to reach limit
            // PUTs/FlushPuts triggers check
            std::unique_lock<std::mutex> queue_lock(hx->p->queues.puts.mutex);
            hx->p->queues.puts.start_processing.wait(
                queue_lock,
                [hx]() -> bool {
                    return (!hx->p->running ||
                            (hx->p->queues.puts.count >= hx->p->async_puts.max_queued) ||
                            hx->p->queues.puts.flushed);
                }
            );

            // puts.mutex now locked

            // move PUTs to local variable
            puts = std::move(hx->p->queues.puts.queue);

            // reset PUT queues
            hx->p->queues.puts.queue.clear();
            hx->p->queues.puts.queue.resize(hx->p->range_server.total_range_servers);
            hx->p->queues.puts.count = 0;
            hx->p->queues.puts.flushed = false;
        }

        // puts.mutex unlocked
        // new PUTs can enter queue while processing occurs

        // process PUTs
        hxhim::Results *results = hxhim::process<Message::Request::BPut, Message::Response::BPut>(hx, puts);

        // store results in hxhim instance
        {
            if (hx->p->async_puts.results) {
                hx->p->async_puts.results->Append(results);
                destruct(results);
            }
            else {
                hx->p->async_puts.results = results;
            }

            std::unique_lock<std::mutex> async_lock(hx->p->async_puts.mutex);
            hx->p->async_puts.done_check = true;
        }

        hx->p->async_puts.done.notify_all();
    }

    mlog(HXHIM_CLIENT_DBG, "Background PUT thread stopping");
}

/**
 * async_puts
 * Starts up the background thread that does asynchronous PUTs.
 *
 * @param hx   the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::async_puts(hxhim_t *hx) {
    if (init::running(hx) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    if (hx->p->async_puts.enabled) {
        // Set up queued PUT results list
        hx->p->async_puts.results = nullptr;

        // Start the background thread
        hx->p->async_puts.thread = std::thread(async_put_thread, hx);
        hxhim::wait_for_background_puts(hx);
    }

    return HXHIM_SUCCESS;
}

/**
 * hash
 * Sets up the hash algorithm and extra arguments
 *
 * @param hx             the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::hash(hxhim_t *) {
    mlog(HXHIM_CLIENT_INFO, "Starting Hash Initalization");
    mlog(HXHIM_CLIENT_INFO, "Completed Hash Initalization");
    return HXHIM_SUCCESS;
}

/**
 * transport
 * Starts up the transport layer
 *
 * @param hx   the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::transport(hxhim_t *hx) {
    mlog(HXHIM_CLIENT_INFO, "Starting Transport Initialization");

    if (!hx->p->range_server.client_ratio) {
        mlog(HXHIM_CLIENT_ERR, "Client ratio must be at least 1");
        return HXHIM_ERROR;
    }

    if (!hx->p->range_server.server_ratio) {
        mlog(HXHIM_SERVER_ERR, "Server ratio must be at least 1");
        return HXHIM_ERROR;
    }

    const int ret = Transport::init(hx,
                                    hx->p->range_server.client_ratio,
                                    hx->p->range_server.server_ratio,
                                    hx->p->transport.endpointgroup,
                                    hx->p->transport.config);

    mlog(HXHIM_CLIENT_INFO, "Completed Transport Initialization");
    return (ret == TRANSPORT_SUCCESS)?HXHIM_SUCCESS:HXHIM_ERROR;
}
