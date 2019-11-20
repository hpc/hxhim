#include <algorithm>
#include <cfloat>
#include <cmath>
#include <cstring>
#include <functional>
#include <memory>
#include <unordered_map>

#include "hxhim/Results_private.hpp"
#include "hxhim/config.hpp"
#include "hxhim/hxhim.h"
#include "hxhim/hxhim.hpp"
#include "hxhim/local_client.hpp"
#include "hxhim/options_private.hpp"
#include "hxhim/private.hpp"
#include "hxhim/range_server.hpp"
#include "hxhim/shuffle.hpp"
#include "utils/macros.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

/**
 * Open
 * Start a HXHIM session
 *
 * @param hx   the HXHIM session
 * @param opts the HXHIM options to use
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Open(hxhim_t *hx, hxhim_options_t *opts) {
    if (!hx || !opts || !opts->p) {
        return HXHIM_ERROR;
    }

    //Open mlog - stolen from plfs
    mlog_open((char *) "hxhim", 0, opts->p->debug_level, opts->p->debug_level, nullptr, 0, MLOG_LOGPID, 0);

    mlog(HXHIM_CLIENT_INFO, "Initializing HXHIM");

    if (!(hx->p = new hxhim_private_t())) {
        mlog(HXHIM_CLIENT_ERR, "Failed to allocate space for private HXHIM data");
        return HXHIM_ERROR;
    }

    if ((init::bootstrap  (hx, opts) != HXHIM_SUCCESS) ||
        (init::running    (hx, opts) != HXHIM_SUCCESS) ||
        (init::memory     (hx, opts) != HXHIM_SUCCESS) ||
        (init::hash       (hx, opts) != HXHIM_SUCCESS) ||
        (init::datastore  (hx, opts) != HXHIM_SUCCESS) ||
        (init::async_put  (hx, opts) != HXHIM_SUCCESS) ||
        (init::transport  (hx, opts) != HXHIM_SUCCESS)) {
        MPI_Barrier(hx->p->bootstrap.comm);
        Close(hx);
        mlog(HXHIM_CLIENT_ERR, "Failed to initialize HXHIM");
        return HXHIM_ERROR;
    }

    mlog(HXHIM_CLIENT_INFO, "Waiting for everyone to complete initialization");
    MPI_Barrier(hx->p->bootstrap.comm);
    mlog(HXHIM_CLIENT_INFO, "Successfully initialized HXHIM on rank %d/%d", hx->p->bootstrap.rank, hx->p->bootstrap.size);

    return HXHIM_SUCCESS;
}

/**
 * hxhimOpen
 * Start a HXHIM session
 *
 * @param hx             the HXHIM session
 * @param bootstrap_comm the MPI communicator used to boostrap MDHIM
 * @param filename       the name of the file to open (for now, it is only the mdhim configuration)
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimOpen(hxhim_t *hx, hxhim_options_t *opts) {
    return hxhim::Open(hx, opts);
}

/**
 * OpenOne
 * Starts a HXHIM session with only 1 backend datastore.
 * This can only be called when the world size is 1.
 *
 * @param hx       the HXHIM session
 * @param opts     the HXHIM options to use
 * @param db_path  the name of the datastore to pass
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::OpenOne(hxhim_t *hx, hxhim_options_t *opts, const std::string &db_path) {
    if (!hx || !opts || !opts->p) {
        return HXHIM_ERROR;
    }

    //Open mlog - stolen from plfs
    mlog_open((char *) "hxhim", 0, opts->p->debug_level, opts->p->debug_level, nullptr, 0, MLOG_LOGPID, 0);

    if (!(hx->p = new hxhim_private_t())) {
        mlog(HXHIM_CLIENT_ERR, "Failed to allocate space for private HXHIM data");
        return HXHIM_ERROR;
    }

    if ((init::bootstrap     (hx, opts)          != HXHIM_SUCCESS) ||
        (hx->p->bootstrap.size                   != 1)             || // Only allow for 1 rank
        (init::running       (hx, opts)          != HXHIM_SUCCESS) ||
        (init::memory        (hx, opts)          != HXHIM_SUCCESS) ||
        (init::hash          (hx, opts)          != HXHIM_SUCCESS) ||
        (init::one_datastore (hx, opts, db_path) != HXHIM_SUCCESS) ||
        (init::async_put     (hx, opts)          != HXHIM_SUCCESS)) {
        MPI_Barrier(hx->p->bootstrap.comm);
        Close(hx);
        mlog(HXHIM_CLIENT_ERR, "Failed to initialize HXHIM");
        return HXHIM_ERROR;
    }

    mlog(HXHIM_CLIENT_INFO, "Successfully initialized HXHIM");
    MPI_Barrier(hx->p->bootstrap.comm);
    return HXHIM_SUCCESS;
}

/**
 * OpenOne
 * Starts a HXHIM session with only 1 backend datastore
 *
 * @param hx       the HXHIM session
 * @param opts     the HXHIM options to use
 * @param db_path  the name of the datastore to pass
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimOpenOne(hxhim_t *hx, hxhim_options_t *opts, const char *db_path, const size_t db_path_len) {
    return hxhim::OpenOne(hx, opts, std::string(db_path, db_path_len));
}

/**
 * Close
 * Terminates a HXHIM session
 *
 * @param hx the HXHIM session
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Close(hxhim_t *hx) {
    mlog(HXHIM_CLIENT_INFO, "Starting to shutdown HXHIM");
    if (!valid(hx)) {
        mlog(HXHIM_CLIENT_ERR, "Bad HXHIM instance");
        return HXHIM_ERROR;
    }

    destroy::running(hx);
    Results::Destroy(hx, hxhim::Sync(hx));
    destroy::async_put(hx);

    mlog(HXHIM_CLIENT_DBG, "Waiting for all ranks to complete syncing");
    MPI_Barrier(hx->p->bootstrap.comm);
    mlog(HXHIM_CLIENT_DBG, "Closing HXHIM");

    destroy::transport(hx);
    destroy::hash(hx);
    destroy::datastore(hx);
    destroy::memory(hx);
    destroy::bootstrap(hx);

    // clean up pointer to private data
    delete hx->p;
    hx->p = nullptr;

    mlog(HXHIM_CLIENT_INFO, "HXHIM has been shutdown");
    mlog_close();
    return HXHIM_SUCCESS;
}

/**
 * hxhimClose
 * Terminates a HXHIM session
 *
 * @param hx the HXHIM session
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimClose(hxhim_t *hx) {
    return hxhim::Close(hx);
}

/**
 * FlushPuts
 * Flushes all queued PUTs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session
 * @return results from sending the PUTs
 */
hxhim::Results *hxhim::FlushPuts(hxhim_t *hx) {
    mlog(HXHIM_CLIENT_DBG, "Flushing PUTs");
    if (!valid(hx)) {
        return nullptr;
    }

    mlog(HXHIM_CLIENT_DBG, "Emptying PUT queue");

    hxhim::Unsent<hxhim::PutData> &unsent = hx->p->queues.puts;
    {
        std::unique_lock<std::mutex> lock(unsent.mutex);
        unsent.force = true;
    }
    unsent.start_processing.notify_all();

    mlog(HXHIM_CLIENT_DBG, "Forcing flush %d", unsent.force);

    // wait for flush to complete
    std::unique_lock<std::mutex> lock(unsent.mutex);
    while (hx->p->running && unsent.force) {
        mlog(HXHIM_CLIENT_DBG, "Waiting for PUT queue to be processed %d", unsent.force);
        unsent.done_processing.wait(lock, [&](){ return !hx->p->running || !unsent.force; });
    }

    mlog(HXHIM_CLIENT_DBG, "Emptied out PUT queue");

    std::unique_lock<std::mutex> results_lock(hx->p->async_put.mutex);

    mlog(HXHIM_CLIENT_DBG, "Processing PUT results");

    // return PUT results and allocate space for new PUT results
    hxhim::Results *res = hx->p->async_put.results;
    hx->p->async_put.results = hx->p->memory_pools.results->acquire<hxhim::Results>(hx);

    mlog(HXHIM_CLIENT_DBG, "PUTs Flushed");
    return res;
}

/**
 * hxhimFlushPuts
 * Flushes all queued PUTs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session
 * @return Pointer to return value wrapper
 */
hxhim_results_t *hxhimFlushPuts(hxhim_t *hx) {
    return hxhim_results_init(hx, hxhim::FlushPuts(hx));
}

/**
 * get_core
 * The core set of function calls that are needed to send Gets to datastores and receive responses
 *
 * @param hx        the HXHIM session
 * @param curr      the current set of data to process
 * @param count     the number of elements in this set of data
 * @param local     the buffer for local data  (not a local variable in order to avoid allocations)
 * @param remote    the buffer for remote data (not a local variable in order to avoid allocations)
 * @return results from sending the GETs
 */
static hxhim::Results *get_core(hxhim_t *hx,
                                hxhim::GetData *head) {
    mlog(HXHIM_CLIENT_DBG, "Start get_core");

    if (!head) {
        return nullptr;
    }

    // serialized results
    hxhim::Results *res = hx->p->memory_pools.results->acquire<hxhim::Results>(hx);

    // declare local GET requests here to not reallocate every loop
    Transport::Request::BGet local(hx->p->memory_pools.arrays, hx->p->memory_pools.buffers, hx->p->max_ops_per_send.gets);
    local.src = hx->p->bootstrap.rank;
    local.dst = hx->p->bootstrap.rank;

    // maximum number of remote destinations allowed at any time
    static const std::size_t max_remote = std::max(hx->p->memory_pools.requests->regions() / 2, (std::size_t) 1);

    while (hx->p->running && head) {
        // current set of remote destinations to send to
        std::unordered_map<int, Transport::Request::BGet *> remote;

        // reset local without deallocating memory
        local.count = 0;

        hxhim::GetData *curr = head;

        while (hx->p->running && curr) {
            if (hxhim::shuffle::Get(hx, hx->p->max_ops_per_send.gets,
                                    curr->subject, curr->subject_len,
                                    curr->predicate, curr->predicate_len,
                                    curr->object_type,
                                    &local,
                                    remote,
                                    max_remote) > -1) {
                // head node
                if (curr == head) {
                    head = curr->next;
                }

                // there is a node before the current one
                if (curr->prev) {
                    curr->prev->next = curr->next;
                }

                // there is a node after the current one
                if (curr->next) {
                    curr->next->prev = curr->prev;
                }

                hxhim::GetData *next = curr->next;

                curr->prev = nullptr;
                curr->next = nullptr;

                // deallocate current node
                hx->p->memory_pools.ops_cache->release(curr);
                curr = next;
            }
            else {
                curr = curr->next;
            }
        }

        // GET the batch
        if (hx->p->running && remote.size()) {
            hxhim::collect_fill_stats(remote, hx->p->stats.bget);
            Transport::Response::BGet *responses = hx->p->transport->BGet(remote);
            for(Transport::Response::BGet *curr = responses; curr; curr = Transport::next(curr, hx->p->memory_pools.responses)) {
                for(std::size_t i = 0; i < curr->count; i++) {
                    res->Add(hxhim::Result::init(hx, curr, i));
                }
            }
        }

        for(decltype(remote)::value_type const &dst : remote) {
            hx->p->memory_pools.requests->release(dst.second);
        }

        if (hx->p->running && local.count) {
            hxhim::collect_fill_stats(&local, hx->p->stats.bget);
            Transport::Response::BGet *responses = local_client_bget(hx, &local);
            for(Transport::Response::BGet *curr = responses; curr; curr = Transport::next(curr, hx->p->memory_pools.responses)) {
                for(std::size_t i = 0; i < curr->count; i++) {
                    res->Add(hxhim::Result::init(hx, curr, i));
                }
            }
        }
    }

    return res;
}

/**
 * get_core
 * The core set of function calls that are needed to send Gets to datastores and receive responses
 *
 * @param hx        the HXHIM session
 * @param curr      the current set of data to process
 * @param count     the number of elements in this set of data
 * @param local     the buffer for local data  (not a local variable in order to avoid allocations)
 * @param remote    the buffer for remote data (not a local variable in order to avoid allocations)
 * @return results from sending the GETs
 */
static hxhim::Results *get2_core(hxhim_t *hx,
                                hxhim::GetData2 *head) {
    mlog(HXHIM_CLIENT_DBG, "Start get2_core");

    if (!head) {
        return nullptr;
    }

    // serialized results
    hxhim::Results *res = hx->p->memory_pools.results->acquire<hxhim::Results>(hx);

    // declare local GET requests here to not reallocate every loop
    Transport::Request::BGet2 local(hx->p->memory_pools.arrays, hx->p->memory_pools.buffers, hx->p->max_ops_per_send.gets);
    local.src = hx->p->bootstrap.rank;
    local.dst = hx->p->bootstrap.rank;

    // maximum number of remote destinations allowed at any time
    static const std::size_t max_remote = std::max(hx->p->memory_pools.requests->regions() / 2, (std::size_t) 1);

    while (hx->p->running && head) {
        // current set of remote destinations to send to
        std::unordered_map<int, Transport::Request::BGet2 *> remote;

        // reset local without deallocating memory
        local.count = 0;

        hxhim::GetData2 *curr = head;

        while (hx->p->running && curr) {
            if (hxhim::shuffle::Get2(hx, hx->p->max_ops_per_send.gets,
                                     curr->subject, curr->subject_len,
                                     curr->predicate, curr->predicate_len,
                                     curr->object_type, curr->object, curr->object_len,
                                     &local,
                                     remote,
                                     max_remote) > -1) {
                // head node
                if (curr == head) {
                    head = curr->next;
                }

                // there is a node before the current one
                if (curr->prev) {
                    curr->prev->next = curr->next;
                }

                // there is a node after the current one
                if (curr->next) {
                    curr->next->prev = curr->prev;
                }

                hxhim::GetData2 *next = curr->next;

                curr->prev = nullptr;
                curr->next = nullptr;

                // deallocate current node
                hx->p->memory_pools.ops_cache->release(curr);
                curr = next;
            }
            else {
                curr = curr->next;
            }
        }

        // // GET the batch
        // if (hx->p->running && remote.size()) {
        //     hxhim::collect_fill_stats(remote, hx->p->stats.bget);
        //     Transport::Response::BGet2 *responses = hx->p->transport->BGet2(remote);
        //     for(Transport::Response::BGet2 *curr = responses; curr; curr = Transport::next(curr, hx->p->memory_pools.responses)) {
        //         for(std::size_t i = 0; i < curr->count; i++) {
        //             res->Add(hxhim::Result::init(hx, curr, i));
        //         }
        //     }
        // }

        // for(decltype(remote)::value_type const &dst : remote) {
        //     hx->p->memory_pools.requests->release(dst.second);
        // }

        if (hx->p->running && local.count) {
            hxhim::collect_fill_stats(&local, hx->p->stats.bget);
            Transport::Response::BGet2 *responses = local_client_bget2(hx, &local);
            for(Transport::Response::BGet2 *curr = responses; curr; curr = Transport::next(curr, hx->p->memory_pools.responses)) {
                for(std::size_t i = 0; i < curr->count; i++) {
                    res->Add(hxhim::Result::init(hx, curr, i));
                }
            }
        }
    }

    return res;
}

/**
 * FlushGets
 * Flushes all queued GETs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session
 * @return Pointer to return value wrapper
 */
hxhim::Results *hxhim::FlushGets(hxhim_t *hx) {
    mlog(HXHIM_CLIENT_DBG, "Flushing GETs");
    if (!valid(hx)) {
        return nullptr;
    }

    hxhim::Unsent<hxhim::GetData> &gets = hx->p->queues.gets;
    hxhim::GetData *curr = nullptr;
    {
        std::lock_guard<std::mutex> lock(gets.mutex);
        curr = gets.head;
        gets.head = nullptr;
        gets.tail = nullptr;
    }

    return get_core(hx, curr);
}

/**
 * hxhimFlushGets
 * Flushes all queued GETs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session
 * @return Pointer to return value wrapper
 */
hxhim_results_t *hxhimFlushGets(hxhim_t *hx) {
    return hxhim_results_init(hx, hxhim::FlushGets(hx));
}

/**
 * FlushGets
 * Flushes all queued GETs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session
 * @return Pointer to return value wrapper
 */
hxhim::Results *hxhim::FlushGets2(hxhim_t *hx) {
    mlog(HXHIM_CLIENT_DBG, "Flushing GETs");
    if (!valid(hx)) {
        return nullptr;
    }

    hxhim::Unsent<hxhim::GetData2> &gets = hx->p->queues.gets2;
    hxhim::GetData2 *curr = nullptr;
    {
        std::lock_guard<std::mutex> lock(gets.mutex);
        curr = gets.head;
        gets.head = nullptr;
        gets.tail = nullptr;
    }

    return get2_core(hx, curr);
}

/**
 * hxhimFlushGets
 * Flushes all queued GETs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session
 * @return Pointer to return value wrapper
 */
hxhim_results_t *hxhimFlushGets2(hxhim_t *hx) {
    return hxhim_results_init(hx, hxhim::FlushGets2(hx));
}

/**
 * getop_core
 * The core set of function calls that are needed to send GetOps to datastores and receive responses
 *
 * @param hx        the HXHIM session
 * @param curr      the current set of data to process
 * @param count     the number of elements in this set of data
 * @param local     the buffer for local data  (not a local variable in order to avoid allocations)
 * @param remote    the buffer for remote data (not a local variable in order to avoid allocations)
 * @return results from sending the GETOPs
 */
static hxhim::Results *getop_core(hxhim_t *hx,
                                  hxhim::GetOpData *head) {
    mlog(HXHIM_CLIENT_DBG, "Start getopt_core");

    if (!head) {
        return nullptr;
    }

    // serialized results
    hxhim::Results *res = hx->p->memory_pools.results->acquire<hxhim::Results>(hx);

    // declare local GETOP requests here to not reallocate every loop
    Transport::Request::BGetOp local(hx->p->memory_pools.arrays, hx->p->memory_pools.buffers, hx->p->max_ops_per_send.getops);
    local.src = hx->p->bootstrap.rank;
    local.dst = hx->p->bootstrap.rank;

    // maximum number of remote destinations allowed at any time
    static const std::size_t max_remote = std::max(hx->p->memory_pools.requests->regions() / 2, (std::size_t) 1);

    while (hx->p->running && head) {
        // current set of remote destinations to send to
        std::unordered_map<int, Transport::Request::BGetOp *> remote;

        // reset local without deallocating memory
        local.count = 0;

        hxhim::GetOpData *curr = head;

        while (hx->p->running && curr) {
            if (hxhim::shuffle::GetOp(hx, hx->p->max_ops_per_send.gets,
                                      curr->subject, curr->subject_len,
                                      curr->predicate, curr->predicate_len,
                                      curr->object_type,
                                      curr->num_recs, curr->op,
                                      &local,
                                      remote,
                                      max_remote) > -1) {
                // head node
                if (curr == head) {
                    head = curr->next;
                }

                // there is a node before the current one
                if (curr->prev) {
                    curr->prev->next = curr->next;
                }

                // there is a node after the current one
                if (curr->next) {
                    curr->next->prev = curr->prev;
                }

                hxhim::GetOpData *next = curr->next;

                curr->prev = nullptr;
                curr->next = nullptr;

                // deallocate current node
                hx->p->memory_pools.ops_cache->release(curr);
                curr = next;
            }
            else {
                curr = curr->next;
            }
        }

        // GET the batch
        if (hx->p->running && remote.size()) {
            hxhim::collect_fill_stats(remote, hx->p->stats.bgetop);
            Transport::Response::BGetOp *responses = hx->p->transport->BGetOp(remote);
            for(Transport::Response::BGetOp *curr = responses; curr; curr = Transport::next(curr, hx->p->memory_pools.responses)) {
                for(std::size_t i = 0; i < curr->count; i++) {
                    res->Add(hxhim::Result::init(hx, curr, i));
                }
            }
        }

        for(decltype(remote)::value_type const &dst : remote) {
            hx->p->memory_pools.requests->release(dst.second);
        }

        if (hx->p->running && local.count) {
            hxhim::collect_fill_stats(&local, hx->p->stats.bgetop);
            Transport::Response::BGetOp *responses = local_client_bget_op(hx, &local);
            for(Transport::Response::BGetOp *curr = responses; curr; curr = Transport::next(curr, hx->p->memory_pools.responses)) {
                for(std::size_t i = 0; i < curr->count; i++) {
                    res->Add(hxhim::Result::init(hx, curr, i));
                }
            }
        }
    }

    return res;
}

/**
 * FlushGetOps
 * Flushes all queued GETs with specific operations
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session
 * @return Pointer to return value wrapper
 */
hxhim::Results *hxhim::FlushGetOps(hxhim_t *hx) {
    if (!valid(hx)) {
        return nullptr;
    }

    hxhim::Unsent<hxhim::GetOpData> &getops = hx->p->queues.getops;
    std::lock_guard<std::mutex> lock(getops.mutex);

    hxhim::GetOpData *curr = getops.head;
    getops.head = nullptr;
    getops.tail = nullptr;

    return getop_core(hx, curr);
}

/**
 * hxhimFlushGetOps
 * Flushes all queued GETs with specific operations
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session
 * @return Pointer to return value wrapper
 */
hxhim_results_t *hxhimFlushGetOps(hxhim_t *hx) {
    return hxhim_results_init(hx, hxhim::FlushGetOps(hx));
}

/**
 * delete_core
 * The core set of function calls that are needed to send Deletes to datastores and receive responses
 *
 * @param hx        the HXHIM session
 * @param curr      the current set of data to process
 * @param count     the number of elements in this set of data
 * @param local     the buffer for local data  (not a local variable in order to avoid allocations)
 * @param remote    the buffer for remote data (not a local variable in order to avoid allocations)
 * @return results from sending the DELs
 */
static hxhim::Results *delete_core(hxhim_t *hx,
                                   hxhim::DeleteData *head) {
    mlog(HXHIM_CLIENT_DBG, "Start delete_core");

    if (!head) {
        return nullptr;
    }

    // serialized results
    hxhim::Results *res = hx->p->memory_pools.results->acquire<hxhim::Results>(hx);

    // declare local DELETE requests here to not reallocate every loop
    Transport::Request::BDelete local(hx->p->memory_pools.arrays, hx->p->memory_pools.buffers, hx->p->max_ops_per_send.gets);
    local.src = hx->p->bootstrap.rank;
    local.dst = hx->p->bootstrap.rank;

    // maximum number of remote destinations allowed at any time
    static const std::size_t max_remote = std::max(hx->p->memory_pools.requests->regions() / 2, (std::size_t) 1);

    while (hx->p->running && head) {
        // current set of remote destinations to send to
        std::unordered_map<int, Transport::Request::BDelete *> remote;

        // reset local without deallocating memory
        local.count = 0;

        hxhim::DeleteData *curr = head;

        while (hx->p->running && curr) {
            if (hxhim::shuffle::Delete(hx, hx->p->max_ops_per_send.deletes,
                                    curr->subject, curr->subject_len,
                                    curr->predicate, curr->predicate_len,
                                    &local,
                                    remote,
                                    max_remote) > -1) {
                // head node
                if (curr == head) {
                    head = curr->next;
                }

                // there is a node before the current one
                if (curr->prev) {
                    curr->prev->next = curr->next;
                }

                // there is a node after the current one
                if (curr->next) {
                    curr->next->prev = curr->prev;
                }

                hxhim::DeleteData *next = curr->next;

                curr->prev = nullptr;
                curr->next = nullptr;

                // deallocate current node
                hx->p->memory_pools.ops_cache->release(curr);
                curr = next;
            }
            else {
                curr = curr->next;
            }
        }

        // DELETE the batch
        if (hx->p->running && remote.size()) {
            hxhim::collect_fill_stats(remote, hx->p->stats.bdel);
            Transport::Response::BDelete *responses = hx->p->transport->BDelete(remote);
            for(Transport::Response::BDelete *curr = responses; curr; curr = Transport::next(curr, hx->p->memory_pools.responses)) {
                for(std::size_t i = 0; i < curr->count; i++) {
                    res->Add(hxhim::Result::init(hx, curr, i));
                }
            }
        }

        for(decltype(remote)::value_type const &dst : remote) {
            hx->p->memory_pools.requests->release(dst.second);
        }

        if (hx->p->running && local.count) {
            hxhim::collect_fill_stats(&local, hx->p->stats.bdel);
            Transport::Response::BDelete *responses = local_client_bdelete(hx, &local);
            for(Transport::Response::BDelete *curr = responses; curr; curr = Transport::next(curr, hx->p->memory_pools.responses)) {
                for(std::size_t i = 0; i < curr->count; i++) {
                    res->Add(hxhim::Result::init(hx, curr, i));
                }
            }
        }
    }

    return res;
}

/**
 * FlushDeletes
 * Flushes all queued DELs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session
 * @return Pointer to return value wrapper
 */
hxhim::Results *hxhim::FlushDeletes(hxhim_t *hx) {
    if (!valid(hx)) {
        return nullptr;
    }

    hxhim::Unsent<hxhim::DeleteData> &dels = hx->p->queues.deletes;
    std::lock_guard<std::mutex> lock(dels.mutex);

    hxhim::DeleteData *curr = dels.head;
    dels.head = nullptr;
    dels.tail = nullptr;

    return delete_core(hx, curr);
}

/**
 * hxhimFlushDeletes
 * Flushes all queued DELs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session
 * @return Pointer to return value wrapper
 */
hxhim_results_t *hxhimFlushDeletes(hxhim_t *hx) {
    return hxhim_results_init(hx, hxhim::FlushDeletes(hx));
}

/**
 * Flush
 *     1. Do all PUTs
 *     2. Do all GETs
 *     3. Do all GET_OPs
 *     4. Do all DELs
 *
 * @param hx the HXHIM session
 * @return A list of results
 */
hxhim::Results *hxhim::Flush(hxhim_t *hx) {
    mlog(HXHIM_CLIENT_DBG, "Flushing HXHIM");
    hxhim::Results *res    = hx->p->memory_pools.results->acquire<hxhim::Results>(hx);

    hxhim::Results *puts   = FlushPuts(hx);
    res->Append(puts);     hx->p->memory_pools.results->release(puts);

    hxhim::Results *gets   = FlushGets(hx);
    res->Append(gets);     hx->p->memory_pools.results->release(gets);

    hxhim::Results *gets2  = FlushGets2(hx);
    res->Append(gets2);    hx->p->memory_pools.results->release(gets2);

    hxhim::Results *getops = FlushGetOps(hx);
    res->Append(getops);   hx->p->memory_pools.results->release(getops);

    hxhim::Results *dels   = FlushDeletes(hx);
    res->Append(dels);     hx->p->memory_pools.results->release(dels);

    mlog(HXHIM_CLIENT_DBG, "Completed Flushing HXHIM");
    return res;
}

/**
 * hxhimFlush
 * Push all queued work into MDHIM
 *
 * @param hx
 * @return An array of results (3 values)
 */
hxhim_results_t *hxhimFlush(hxhim_t *hx) {
    return hxhim_results_init(hx, hxhim::Flush(hx));
}

/**
 * Sync
 * Force all queues to be emptied out and
 * writes all data to the backing storage.
 *
 * @param hx the HXHIM session
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
hxhim::Results *hxhim::Sync(hxhim_t *hx) {
    hxhim::Results *res = Flush(hx);

    MPI_Barrier(hx->p->bootstrap.comm);

    if (hxhim::range_server::is_range_server(hx->p->bootstrap.rank, hx->p->range_server.client_ratio, hx->p->range_server.server_ratio)) {
        // Sync local data stores
        for(std::size_t i = 0; i < hx->p->datastore.count; i++) {
            const int synced = hx->p->datastore.datastores[i]->Sync();
            res->Add(hxhim::Result::init(hx, i, synced));
        }
    }

    MPI_Barrier(hx->p->bootstrap.comm);

    return res;
}

/**
 * hxhimSync
 * Force all queues to be emptied out and
 * writes all data to the backing storage.
 *
 * @param hx the HXHIM session
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
hxhim_results_t *hxhimSync(hxhim_t *hx) {
    return hxhim_results_init(hx, hxhim::Sync(hx));
}

/**
 * ChangeHash
 * Changes the hash function and associated
 * datastores.
 *     - This function is a collective.
 *     - Previously queued operations are flushed
 *     - The datastores are synced before the hash is switched.
 *
 * @param hx   the HXHIM session
 * @param name the name of the new hash function
 * @param func the new hash function
 * @param args the extra args that are used in the hash function
 * @return A list of results
 */
hxhim::Results *hxhim::ChangeHash(hxhim_t *hx, const char *name, hxhim_hash_t func, void *args) {
    hxhim::Results *res = Sync(hx);

    MPI_Barrier(hx->p->bootstrap.comm);

    // change hashes
    hx->p->hash.func = func;
    hx->p->hash.args = args;

    // change datastores
    for(std::size_t i = 0; i < hx->p->datastore.count; i++) {
        std::stringstream s;
        s << name << "-" << hxhim::datastore::get_id(hx, hx->p->bootstrap.rank, i);
        hx->p->datastore.datastores[i]->Open(s.str());
    }

    MPI_Barrier(hx->p->bootstrap.comm);

    return res;
}

/**
 * ChangeHash
 * Changes the hash function and associated
 * datastores.
 *     - This function is a collective.
 *     - Previously queued operations are flushed
 *     - The datastores are synced before the hash is switched.
 *
 * @param hx   the HXHIM session
 * @param name the name of the new hash function
 * @param func the new hash function
 * @param args the extra args that are used in the hash function
 * @return A list of results
 */
hxhim_results_t *hxhimChangeHash(hxhim_t *hx, const char *name, hxhim_hash_t func, void *args) {
    return hxhim_results_init(hx, hxhim::ChangeHash(hx, name, func, args));
}

/**
 * Put
 * Add a PUT into the work queue
 *
 * @param hx             the HXHIM session
 * @param subject        the subject to put
 * @param subject_len    the length of the subject to put
 * @param predicate      the prediate to put
 * @param predicate_len  the length of the prediate to put
 * @param object_type    the type of the object
 * @param object         the object to put
 * @param object_len     the length of the object
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Put(hxhim_t *hx,
               void *subject, std::size_t subject_len,
               void *predicate, std::size_t predicate_len,
               enum hxhim_type_t object_type, void *object, std::size_t object_len) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    return hxhim::PutImpl(hx,
                          subject, subject_len,
                          predicate, predicate_len,
                          object_type, object, object_len);
}

/**
 * hxhimPut
 * Add a PUT into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to put
 * @param subject_len  the length of the subject to put
 * @param prediate     the prediate to put
 * @param prediate_len the length of the prediate to put
 * @param object_type  the type of the object
 * @param object       the object to put
 * @param object_len   the length of the object
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimPut(hxhim_t *hx,
             void *subject, std::size_t subject_len,
             void *predicate, std::size_t predicate_len,
             enum hxhim_type_t object_type, void *object, std::size_t object_len) {
    return hxhim::Put(hx,
                      subject, subject_len,
                      predicate, predicate_len,
                      object_type, object, object_len);
}

/**
 * Get
 * Add a GET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subject        the subject to put
 * @param subject_len    the length of the subject to put
 * @param predicate      the prediate to put
 * @param predicate_len  the length of the prediate to put
 * @param object_type    the type of the object
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Get(hxhim_t *hx,
               void *subject, std::size_t subject_len,
               void *predicate, std::size_t predicate_len,
               enum hxhim_type_t object_type) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    return hxhim::GetImpl(hx,
                          subject, subject_len,
                          predicate, predicate_len,
                          object_type);
}

/**
 * Get
 * Add a GET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subject        the subject to put
 * @param subject_len    the length of the subject to put
 * @param predicate      the prediate to put
 * @param predicate_len  the length of the prediate to put
 * @param object_type    the type of the object
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimGet(hxhim_t *hx,
             void *subject, size_t subject_len,
             void *predicate, size_t predicate_len,
             enum hxhim_type_t object_type) {
    return hxhim::Get(hx,
                      subject, subject_len,
                      predicate, predicate_len,
                      object_type);
}

/**
 * Get
 * Add a GET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subject        the subject to put
 * @param subject_len    the length of the subject to put
 * @param predicate      the prediate to put
 * @param predicate_len  the length of the prediate to put
 * @param object_type    the type of the object
 * @param object         the prediate to put
 * @param object_len     the length of the prediate to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Get2(hxhim_t *hx,
                void *subject, std::size_t subject_len,
                void *predicate, std::size_t predicate_len,
                enum hxhim_type_t object_type, void *object, std::size_t *object_len) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    return hxhim::GetImpl2(hx,
                           subject, subject_len,
                           predicate, predicate_len,
                           object_type, object, object_len);
}

/**
 * Get
 * Add a GET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subject        the subject to put
 * @param subject_len    the length of the subject to put
 * @param predicate      the prediate to put
 * @param predicate_len  the length of the prediate to put
 * @param object_type    the type of the object
 * @param object         the prediate to put
 * @param object_len     the length of the prediate to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimGet2(hxhim_t *hx,
              void *subject, size_t subject_len,
              void *predicate, size_t predicate_len,
              enum hxhim_type_t object_type, void *object, size_t *object_len) {
    return hxhim::Get2(hx,
                       subject, subject_len,
                       predicate, predicate_len,
                       object_type, object, object_len);
}

/**
 * Delete
 * Add a DELETE into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to delete
 * @param subject_len  the length of the subject to delete
 * @param prediate     the prediate to delete
 * @param prediate_len the length of the prediate to delete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Delete(hxhim_t *hx,
                  void *subject, std::size_t subject_len,
                  void *predicate, std::size_t predicate_len) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    return hxhim::DeleteImpl(hx,
                             subject, subject_len,
                             predicate, predicate_len);
}

/**
 * hxhimDelete
 * Add a DELETE into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to delete
 * @param subject_len  the length of the subject to delete
 * @param prediate     the prediate to delete
 * @param prediate_len the length of the prediate to delete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimDelete(hxhim_t *hx,
                void *subject, std::size_t subject_len,
                void *predicate, std::size_t predicate_len) {
    return hxhim::Delete(hx,
                         subject, subject_len,
                         predicate, predicate_len);
}

/**
 * BPut
 * Add a BPUT into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param object_type   the type of the object
 * @param objects       the objects to put
 * @param object_lens   the lengths of the objects to put
 * @param count         the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BPut(hxhim_t *hx,
                void **subjects, std::size_t *subject_lens,
                void **predicates, std::size_t *predicate_lens,
                enum hxhim_type_t *object_types, void **objects, std::size_t *object_lens,
                std::size_t count) {
    if (!valid(hx)  ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens ||
        !objects    || !object_lens) {
        return HXHIM_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        hxhim::PutImpl(hx, subjects[i], subject_lens[i], predicates[i], predicate_lens[i], object_types[i], objects[i], object_lens[i]);
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhimBPut
 * Add a BPUT into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param object_types  the type of the object
 * @param objects       the objects to put
 * @param object_lens   the lengths of the objects to put
 * @param count         the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBPut(hxhim_t *hx,
              void **subjects, std::size_t *subject_lens,
              void **predicates, std::size_t *predicate_lens,
              enum hxhim_type_t *object_types, void **objects, std::size_t *object_lens,
              std::size_t count) {
    return hxhim::BPut(hx,
                       subjects, subject_lens,
                       predicates, predicate_lens,
                       object_types, objects, object_lens,
                       count);
}

/**
 * BGet
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param hx             the HXHIM session
 * @param subjects       the subjects to get
 * @param subject_lens   the lengths of the subjects to get
 * @param predicates     the prediates to get
 * @param predicate_lens the lengths of the prediates to get
 * @param object_types   the types of the objects
 * @param count          the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BGet(hxhim_t *hx,
                void **subjects, std::size_t *subject_lens,
                void **predicates, std::size_t *predicate_lens,
                hxhim_type_t *object_types,
                std::size_t count) {
    if (!valid(hx)  ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens) {
        return HXHIM_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        hxhim::GetImpl(hx, subjects[i], subject_lens[i], predicates[i], predicate_lens[i], object_types[i]);
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhimBGet
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subjects       the subjects to get
 * @param subject_lens   the lengths of the subjects to get
 * @param predicates     the prediates to get
 * @param predicate_lens the lengths of the prediates to get
 * @param object_types   the types of the objects
 * @param count          the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBGet(hxhim_t *hx,
              void **subjects, std::size_t *subject_lens,
              void **predicates, std::size_t *predicate_lens,
              enum hxhim_type_t *object_types,
              std::size_t count) {
    return hxhim::BGet(hx,
                       subjects, subject_lens,
                       predicates, predicate_lens,
                       object_types,
                       count);
}

/**
 * BGet
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param hx             the HXHIM session
 * @param subjects       the subjects to get
 * @param subject_lens   the lengths of the subjects to get
 * @param predicates     the prediates to get
 * @param predicate_lens the lengths of the prediates to get
 * @param object_types   the types of the objects
 * @param count          the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BGet2(hxhim_t *hx,
                 void **subjects, std::size_t *subject_lens,
                 void **predicates, std::size_t *predicate_lens,
                 hxhim_type_t *object_types, void **objects, std::size_t **object_lens,
                 std::size_t count) {
    if (!valid(hx)  ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens ||
        !objects    || !object_lens) {
        return HXHIM_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        hxhim::GetImpl2(hx, subjects[i], subject_lens[i], predicates[i], predicate_lens[i], object_types[i], objects[i], object_lens[i]);
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhimBGet
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subjects       the subjects to get
 * @param subject_lens   the lengths of the subjects to get
 * @param predicates     the prediates to get
 * @param predicate_lens the lengths of the prediates to get
 * @param object_types   the types of the objects
 * @param objects        the prediates to get
 * @param object_lens    the lengths of the prediates to get
 * @param count          the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBGet2(hxhim_t *hx,
               void **subjects, std::size_t *subject_lens,
               void **predicates, std::size_t *predicate_lens,
               enum hxhim_type_t *object_types, void **objects, std::size_t **object_lens,
               std::size_t count) {
    return hxhim::BGet2(hx,
                        subjects, subject_lens,
                        predicates, predicate_lens,
                        object_types, objects, object_lens,
                        count);
}

/**
 * BGetOp
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subjects       the subjects to get
 * @param subject_lens   the lengths of the subjects to get
 * @param predicates     the predicates to get
 * @param predicate_lens the lengths of the predicates to get
 * @param object_type    the type of the object
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BGetOp(hxhim_t *hx,
                  void *subject, size_t subject_len,
                  void *predicate, size_t predicate_len,
                  enum hxhim_type_t object_type,
                  std::size_t num_records, enum hxhim_get_op_t op) {
    if (!valid(hx)  ||
        !subject || !subject_len) {
        return HXHIM_ERROR;
    }

    hxhim::GetOpData *getop = hx->p->memory_pools.ops_cache->acquire<hxhim::GetOpData>();
    getop->subject = subject;
    getop->subject_len = subject_len;
    getop->predicate = predicate;
    getop->predicate_len = predicate_len;
    getop->object_type = object_type;
    getop->num_recs = num_records;
    getop->op = op;

    hxhim::Unsent<hxhim::GetOpData> &getops = hx->p->queues.getops;
    {
        std::lock_guard<std::mutex> lock(getops.mutex);
        getops.insert(getop);
    }
    getops.start_processing.notify_one();

    return HXHIM_SUCCESS;
}

/**
 * hxhimBGetOp
 * Add a BGET into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to get
 * @param subject_lens  the lengths of the subjects to get
 * @param prediates     the prediates to get
 * @param prediate_lens the lengths of the prediates to get
 * @param object_type   the type of the object
 * @param num_records   the number of key value pairs to get back
 * @param op            the operation to do
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBGetOp(hxhim_t *hx,
                void *subject, size_t subject_len,
                void *predicate, size_t predicate_len,
                enum hxhim_type_t object_type,
                std::size_t num_records, enum hxhim_get_op_t op) {
    return hxhim::BGetOp(hx,
                         subject, subject_len,
                         predicate, predicate_len,
                         object_type,
                         num_records, op);
}

/**
 * BDelete
 * Add a BDEL into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to delete
 * @param subject_lens  the lengths of the subjects to delete
 * @param prediates     the prediates to delete
 * @param prediate_lens the lengths of the prediates to delete
 * @param object_type   the type of the object
 * @param count         the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BDelete(hxhim_t *hx,
                   void **subjects, size_t *subject_lens,
                   void **predicates, size_t *predicate_lens,
                   std::size_t count) {
    if (!valid(hx)  ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens) {
        return HXHIM_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        hxhim::DeleteImpl(hx, subjects[i], subject_lens[i], predicates[i], predicate_lens[i]);
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhimBDelete
 * Add a BDEL into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to delete
 * @param subject_lens  the lengths of the subjects to delete
 * @param prediates     the prediates to delete
 * @param prediate_lens the lengths of the prediates to delete
 * @param object_type   the type of the object
 * @param count         the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBDelete(hxhim_t *hx,
                 void **subjects, size_t *subject_lens,
                 void **predicates, size_t *predicate_lens,
                 std::size_t count) {
    return hxhim::BDelete(hx,
                          subjects, subject_lens,
                          predicates, predicate_lens,
                          count);
}

/**
 * GetStats
 * Collective operation
 * Each desired pointer should be preallocated with space for md->size values
 *
 * @param hx             the HXHIM session
 * @param dst_rank       the rank that is collecting the data
 * @param get_put_times  whether or not to get put_times
 * @param put_times      the array of put times from each rank
 * @param get_num_puts   whether or not to get num_puts
 * @param num_puts       the array of number of puts from each rank
 * @param get_get_times  whether or not to get get_times
 * @param get_times      the array of get times from each rank
 * @param get_num_gets   whether or not to get num_gets
 * @param num_gets       the array of number of gets from each rank
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::GetStats(hxhim_t *hx, const int dst_rank,
                    const bool get_put_times, long double *put_times,
                    const bool get_num_puts, std::size_t *num_puts,
                    const bool get_get_times, long double *get_times,
                    const bool get_num_gets, std::size_t *num_gets) {
    #warning this needs to change to send stats from all datastores
    return hx->p->datastore.datastores[0]->GetStats(dst_rank,
                                                    get_put_times, put_times,
                                                    get_num_puts, num_puts,
                                                    get_get_times, get_times,
                                                    get_num_gets, num_gets);

}

/**
 * hxhimGetStats
 * Collective operation
 * Each desired pointer should be preallocated with space for md->size values
 *
 * @param hx             the HXHIM session
 * @param dst_rank       the rank that is collecting the data
 * @param get_put_times  whether or not to get put_times
 * @param put_times      the array of put times from each rank
 * @param get_num_puts   whether or not to get num_puts
 * @param num_puts       the array of number of puts from each rank
 * @param get_get_times  whether or not to get get_times
 * @param get_times      the array of get times from each rank
 * @param get_num_gets   whether or not to get num_gets
 * @param num_gets       the array of number of gets from each rank
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimGetStats(hxhim_t *hx, const int dst_rank,
                  const int get_put_times, long double *put_times,
                  const int get_num_puts, std::size_t *num_puts,
                  const int get_get_times, long double *get_times,
                  const int get_num_gets, std::size_t *num_gets) {
    return hxhim::GetStats(hx, dst_rank,
                           get_put_times, put_times,
                           get_num_puts, num_puts,
                           get_get_times, get_times,
                           get_num_gets, num_gets);
}

/**
 * GetHistogram
 * Get a histogram
 *
 * @param hx          the HXHIM session
 * @param datastore   the ID of the datastore to get the histogram from
 * @return the histogram, inside a hxhim::Results structure
 */
hxhim::Results *hxhim::GetHistogram(hxhim_t *hx, const int datastore) {
    if (!valid(hx) || (datastore < 0)) {
        return nullptr;
    }

    Transport::Request::Histogram request(hx->p->memory_pools.arrays, hx->p->memory_pools.buffers);
    request.src = hx->p->bootstrap.rank;
    request.dst = hxhim::datastore::get_rank(hx, datastore);
    request.ds_offset = hxhim::datastore::get_offset(hx, datastore);;

    Transport::Response::Histogram *response = nullptr;

    // local
    if (request.src == request.dst) {
        response = local_client_histogram(hx, &request);
    }
    // remote
    else {
        response = hx->p->transport->Histogram(&request);
    }

    hxhim::Results *res = hx->p->memory_pools.results->acquire<hxhim::Results>(hx);
    res->Add(hxhim::Result::init(hx, response));
    hx->p->memory_pools.responses->release(response);
    return res;
}

/**
 * GetHistogram
 * Get a histogram
 *
 * @param hx          the HXHIM session
 * @param datastore   the ID of the datastore to get the histogram from
 * @return the histogram, inside a hxhim::Results structure
 */
hxhim_results_t *hxhimGetHistogram(hxhim_t *hx, const int datastore) {
    return hxhim_results_init(hx, hxhim::GetHistogram(hx, datastore));
}

/**
 * GetBHistogram
 * Get multiple histograms
 *
 * @param hx          the HXHIM session
 * @param datastores  the IDs of the datastores to get the histograms from
 * @param count       the number of datastores to get from
 * @return the histogram, inside a hxhim::Results structure
 */
hxhim::Results *hxhim::GetBHistogram(hxhim_t *hx, const int *datastores, const std::size_t count) {
    if (!hxhim::valid(hx)) {
        return nullptr;
    }

    Transport::Request::BHistogram local(hx->p->memory_pools.arrays, hx->p->memory_pools.buffers, count);
    local.src = hx->p->bootstrap.rank;
    local.dst = hx->p->bootstrap.rank;

    // maximum number of remote destinations allowed at any time
    static const std::size_t max_remote = std::max(hx->p->memory_pools.requests->regions() / 2, (std::size_t) 1);


    hxhim::Results *res = hx->p->memory_pools.results->acquire<hxhim::Results>(hx);

    std::size_t i = 0;
    while (i < count) {
        // current set of remote destinations to send to
        std::unordered_map<int, Transport::Request::BHistogram *> remote;

        // reset local without deallocating memory
        local.count = 0;

        // move the data into the appropriate buffers
        while ((i < count) && (hxhim::shuffle::Histogram(hx, hx->p->max_ops_per_send.max,
                                                         datastores[i],
                                                         &local, remote,
                                                         max_remote) > -1)) {
            i++;
        }

        // remote requests
        if (remote.size()) {
            Transport::Response::BHistogram *responses = hx->p->transport->BHistogram(remote);
            for(Transport::Response::BHistogram *curr = responses; curr; curr = Transport::next(curr, hx->p->memory_pools.responses)) {
                for(std::size_t j = 0; j < curr->count; j++) {
                    res->Add(hxhim::Result::init(hx, curr, j));
                }
            }

            for(decltype(remote)::value_type const &dst : remote) {
                hx->p->memory_pools.requests->release(dst.second);
            }
        }

        // local request
        if (local.count) {
            Transport::Response::BHistogram *responses = local_client_bhistogram(hx, &local);
            for(Transport::Response::BHistogram *curr = responses; curr; curr = Transport::next(curr, hx->p->memory_pools.responses)) {
                for(std::size_t j = 0; j < curr->count; j++) {
                    res->Add(hxhim::Result::init(hx, curr, j));
                }
            }
        }
    }

    return res;
}

/**
 * GetBHistogram
 * Get multiple histograms
 *
 * @param hx          the HXHIM session
 * @param datastores  the IDs of the datastores to get the histograms from
 * @param count       the number of datastores to get from
 * @return the histogram, inside a hxhim::Results structure
 */
hxhim_results_t *hxhimBGetHistogram(hxhim_t *hx, const int *datastores, const size_t count) {
    return hxhim_results_init(hx, hxhim::GetBHistogram(hx, datastores, count));
}

/**
 * GetFilled
 * Collective operation
 * Collects statistics from all ranks in the communicator
 *
 * @param comm        the MPI communicator
 * @param rank        the rank of this instance of Transport
 * @param dst_rank    the rank to send to
 * @param get_bput    whether or not to get bput
 * @param bput        the array of bput from each rank
 * @param get_bget    whether or not to get bget
 * @param bget        the array of bget from each rank
 * @param get_bgetop  whether or not to get bgetop
 * @param bgetop      the array of bgetop from each rank
 * @param get_bdel    whether or not to get bdel
 * @param bdel        the array of bdel from each rank
 * @param calc        the function to use to calculate some statistic using the input data
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
 */
static int GetFilled(MPI_Comm comm, const int rank, const int dst_rank,
                     const bool get_bput, long double *bput,
                     const bool get_bget, long double *bget,
                     const bool get_bgetop, long double *bgetop,
                     const bool get_bdel, long double *bdel,
                     const hxhim_private_t::Stats &stats,
                     const std::function<long double(const hxhim_private_t::Stats::Op &)> &calc) {
    MPI_Barrier(comm);

    if (rank == dst_rank) {
        if (get_bput) {
            const long double filled = calc(stats.bput);
            MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, bput, 1, MPI_LONG_DOUBLE, dst_rank, comm);
        }
        if (get_bget) {
            const long double filled = calc(stats.bget);
            MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, bget, 1, MPI_LONG_DOUBLE, dst_rank, comm);
        }
        if (get_bgetop) {
            const long double filled = calc(stats.bgetop);
            MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, bgetop, 1, MPI_LONG_DOUBLE, dst_rank, comm);
        }
        if (get_bdel) {
            const long double filled = calc(stats.bdel);
            MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, bdel, 1, MPI_LONG_DOUBLE, dst_rank, comm);
        }
    }
    else {
        if (get_bput) {
            const long double filled = calc(stats.bput);
            MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, nullptr, 0, MPI_LONG_DOUBLE, dst_rank, comm);
        }
        if (get_bget) {
            const long double filled = calc(stats.bget);
            MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, nullptr, 0, MPI_LONG_DOUBLE, dst_rank, comm);
        }
        if (get_bgetop) {
            const long double filled = calc(stats.bgetop);
            MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, nullptr, 0, MPI_LONG_DOUBLE, dst_rank, comm);
        }
        if (get_bdel) {
            const long double filled = calc(stats.bdel);
            MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, nullptr, 0, MPI_LONG_DOUBLE, dst_rank, comm);
        }
    }

    MPI_Barrier(comm);

    return HXHIM_SUCCESS;
}

/**
 * GetMinFilled
 * Collective operation
 * Collects statistics from all ranks in the communicator
 *
 * @param comm        the MPI communicator
 * @param dst_rank    the rank to send to
 * @param get_bput    whether or not to get bput
 * @param bput        the array of bput from each rank
 * @param get_bget    whether or not to get bget
 * @param bget        the array of bget from each rank
 * @param get_bgetop  whether or not to get bgetop
 * @param bgetop      the array of bgetop from each rank
 * @param get_bdel    whether or not to get bdel
 * @param bdel        the array of bdel from each rank
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
 */
int hxhim::GetMinFilled(hxhim_t *hx, const int dst_rank,
                        const bool get_bput, long double *bput,
                        const bool get_bget, long double *bget,
                        const bool get_bgetop, long double *bgetop,
                        const bool get_bdel, long double *bdel) {
    auto min_filled = [](const hxhim_private_t::Stats::Op &op) -> long double {
        std::lock_guard<std::mutex> lock(op.mutex);
        long double min = op.filled.size()?LDBL_MAX:0;
        for(REF(op.filled)::value_type const &filled : op.filled) {
            min = std::min(min, filled.percent);
        }

        return min;
    };

    return GetFilled(hx->p->bootstrap.comm, hx->p->bootstrap.rank, dst_rank,
                     get_bput, bput,
                     get_bget, bget,
                     get_bgetop, bgetop,
                     get_bdel, bdel,
                     hx->p->stats,
                     min_filled);
}

/**
 * hxhimGetMinFilled
 * Collective operation
 * Collects transport statistics from all ranks in the communicator
 *
 * @param comm        the MPI communicator
 * @param rank        the rank of this instance of Transport
 * @param dst_rank    the rank to send to
 * @param get_bput    whether or not to get bput
 * @param bput        the array of bput from each rank
 * @param get_bget    whether or not to get bget
 * @param bget        the array of bget from each rank
 * @param get_bgetop  whether or not to get bgetop
 * @param bgetop      the array of bgetop from each rank
 * @param get_bdel    whether or not to get bdel
 * @param bdel        the array of bdel from each rank
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
 */
int hxhimGetMinFilled(hxhim_t *hx, const int dst_rank,
                               const int get_bput, long double *bput,
                               const int get_bget, long double *bget,
                               const int get_bgetop, long double *bgetop,
                               const int get_bdel, long double *bdel) {
    return hxhim::GetMinFilled(hx, dst_rank,
                               get_bput, bput,
                               get_bget, bget,
                               get_bgetop, bgetop,
                               get_bdel, bdel);
}

/**
 * GetAverageFilled
 * Collective operation
 * Collects statistics from all ranks in the communicator
 *
 * @param comm        the MPI communicator
 * @param dst_rank    the rank to send to
 * @param get_bput    whether or not to get bput
 * @param bput        the array of bput from each rank
 * @param get_bget    whether or not to get bget
 * @param bget        the array of bget from each rank
 * @param get_bgetop  whether or not to get bgetop
 * @param bgetop      the array of bgetop from each rank
 * @param get_bdel    whether or not to get bdel
 * @param bdel        the array of bdel from each rank
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
 */
int hxhim::GetAverageFilled(hxhim_t *hx, const int dst_rank,
                            const bool get_bput, long double *bput,
                            const bool get_bget, long double *bget,
                            const bool get_bgetop, long double *bgetop,
                            const bool get_bdel, long double *bdel) {
    auto average_filled = [](const hxhim_private_t::Stats::Op &op) -> long double {
        std::lock_guard<std::mutex> lock(op.mutex);
        long double sum = 0;
        for(REF(op.filled)::value_type const &filled : op.filled) {
            sum += filled.percent;
        }

        return op.filled.size()?(sum / op.filled.size()):0;
    };

    return GetFilled(hx->p->bootstrap.comm, hx->p->bootstrap.rank, dst_rank,
                     get_bput, bput,
                     get_bget, bget,
                     get_bgetop, bgetop,
                     get_bdel, bdel,
                     hx->p->stats,
                     average_filled);
}

/**
 * hxhimGetAverageFilled
 * Collective operation
 * Collects transport statistics from all ranks in the communicator
 *
 * @param comm        the MPI communicator
 * @param rank        the rank of this instance of Transport
 * @param dst_rank    the rank to send to
 * @param get_bput    whether or not to get bput
 * @param bput        the array of bput from each rank
 * @param get_bget    whether or not to get bget
 * @param bget        the array of bget from each rank
 * @param get_bgetop  whether or not to get bgetop
 * @param bgetop      the array of bgetop from each rank
 * @param get_bdel    whether or not to get bdel
 * @param bdel        the array of bdel from each rank
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
 */
int hxhimGetAverageFilled(hxhim_t *hx, const int dst_rank,
                          const int get_bput, long double *bput,
                          const int get_bget, long double *bget,
                          const int get_bgetop, long double *bgetop,
                          const int get_bdel, long double *bdel) {
    return hxhim::GetAverageFilled(hx, dst_rank,
                                   get_bput, bput,
                                   get_bget, bget,
                                   get_bgetop, bgetop,
                                   get_bdel, bdel);
}

/**
 * GetMaxFilled
 * Collective operation
 * Collects statistics from all ranks in the communicator
 *
 * @param comm        the MPI communicator
 * @param dst_rank    the rank to send to
 * @param get_bput    whether or not to get bput
 * @param bput        the array of bput from each rank
 * @param get_bget    whether or not to get bget
 * @param bget        the array of bget from each rank
 * @param get_bgetop  whether or not to get bgetop
 * @param bgetop      the array of bgetop from each rank
 * @param get_bdel    whether or not to get bdel
 * @param bdel        the array of bdel from each rank
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
 */
int hxhim::GetMaxFilled(hxhim_t *hx, const int dst_rank,
                        const bool get_bput, long double *bput,
                        const bool get_bget, long double *bget,
                        const bool get_bgetop, long double *bgetop,
                        const bool get_bdel, long double *bdel) {
    auto max_filled = [](const hxhim_private_t::Stats::Op &op) -> long double {
        std::lock_guard<std::mutex> lock(op.mutex);
        long double max = 0;
        for(REF(op.filled)::value_type const &filled : op.filled) {
            max = std::max(max, filled.percent);
        }

        return max;
    };

    return GetFilled(hx->p->bootstrap.comm, hx->p->bootstrap.rank, dst_rank,
                     get_bput, bput,
                     get_bget, bget,
                     get_bgetop, bgetop,
                     get_bdel, bdel,
                     hx->p->stats,
                     max_filled);
}

/**
 * hxhimGetMaxFilled
 * Collective operation
 * Collects transport statistics from all ranks in the communicator
 *
 * @param comm        the MPI communicator
 * @param rank        the rank of this instance of Transport
 * @param dst_rank    the rank to send to
 * @param get_bput    whether or not to get bput
 * @param bput        the array of bput from each rank
 * @param get_bget    whether or not to get bget
 * @param bget        the array of bget from each rank
 * @param get_bgetop  whether or not to get bgetop
 * @param bgetop      the array of bgetop from each rank
 * @param get_bdel    whether or not to get bdel
 * @param bdel        the array of bdel from each rank
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
 */
int hxhimGetMaxFilled(hxhim_t *hx, const int dst_rank,
                      const int get_bput, long double *bput,
                      const int get_bget, long double *bget,
                      const int get_bgetop, long double *bgetop,
                      const int get_bdel, long double *bdel) {
    return hxhim::GetMaxFilled(hx, dst_rank,
                               get_bput, bput,
                               get_bget, bget,
                               get_bgetop, bgetop,
                               get_bdel, bdel);
}
