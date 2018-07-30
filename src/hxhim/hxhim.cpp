#include <algorithm>
#include <cmath>
#include <cstring>
#include <memory>

#include "hxhim/Results_private.hpp"
#include "hxhim/hxhim.h"
#include "hxhim/hxhim.hpp"
#include "hxhim/local_client.hpp"
#include "hxhim/options_private.hpp"
#include "hxhim/private.hpp"
#include "hxhim/shuffle.hpp"
#include "hxhim/triplestore.hpp"

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

    hx->mpi = opts->p->mpi;

    if (!(hx->p = new hxhim_private_t())) {
        return HXHIM_ERROR;
    }

    if ((init::running     (hx, opts) != HXHIM_SUCCESS) ||
        (init::database    (hx, opts) != HXHIM_SUCCESS) ||
        (init::async_put   (hx, opts) != HXHIM_SUCCESS) ||
        (init::hash        (hx, opts) != HXHIM_SUCCESS) ||
        (init::transport   (hx, opts) != HXHIM_SUCCESS)) {
        MPI_Barrier(hx->mpi.comm);
        Close(hx);
        return HXHIM_ERROR;
    }

    MPI_Barrier(hx->mpi.comm);
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
 * Starts a HXHIM session with only 1 backend database.
 * This can only be called when the world size is 1.
 *
 * @param hx       the HXHIM session
 * @param opts     the HXHIM options to use
 * @param db_path  the name of the database to pass
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::OpenOne(hxhim_t *hx, hxhim_options_t *opts, const std::string &db_path) {
    if (!hx || !opts || !opts->p) {
        return HXHIM_ERROR;
    }

    // Only allow for 1 rank
    if (opts->p->mpi.size != 1) {
        return HXHIM_ERROR;
    }

    hx->mpi = opts->p->mpi;

    if (!(hx->p = new hxhim_private_t())) {
        return HXHIM_ERROR;
    }

    if ((init::running     (hx, opts)          != HXHIM_SUCCESS) ||
        (init::one_database (hx, opts, db_path) != HXHIM_SUCCESS) ||
        (init::async_put   (hx, opts)          != HXHIM_SUCCESS) ||
        (init::hash        (hx, opts)          != HXHIM_SUCCESS)) {
        MPI_Barrier(hx->mpi.comm);
        Close(hx);
        return HXHIM_ERROR;
    }

    MPI_Barrier(hx->mpi.comm);
    return HXHIM_SUCCESS;
}

/**
 * OpenOne
 * Starts a HXHIM session with only 1 backend database
 *
 * @param hx       the HXHIM session
 * @param opts     the HXHIM options to use
 * @param db_path  the name of the database to pass
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
    if (!hx || !hx->p) {
        return HXHIM_ERROR;
    }

    MPI_Barrier(hx->mpi.comm);

    destroy::running(hx);
    destroy::transport(hx);
    destroy::hash(hx);
    destroy::async_put(hx);
    destroy::database(hx);

    // clean up pointer to private data
    delete hx->p;
    hx->p = nullptr;

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
 * Commit
 * Commits all flushed data to disk
 *
 * @param hx the HXHIM session
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Commit(hxhim_t *hx) {
    if (!hx || !hx->p || !hx->p->databases) {
        return HXHIM_ERROR;
    }

    for(std::size_t i = 0; i < hx->p->database_count; i++) {
        if (hx->p->databases[i]->Commit() != HXHIM_SUCCESS) {
            return HXHIM_ERROR;
        }
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhimCommit
 * Commits all flushed data to disk
 *
 * @param hx the HXHIM session
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimCommit(hxhim_t *hx) {
    return hxhim::Commit(hx);
}

/**
 * StatFlush
 * Flushes the MDHIM statistics
 * Mainly needed for BGetOp
 *
 * @param hx the HXHIM session
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::StatFlush(hxhim_t *hx) {
    if (!hx || !hx->p || !hx->p->databases) {
        return HXHIM_ERROR;
    }

    for(std::size_t i = 0; i < hx->p->database_count; i++) {
        if (hx->p->databases[i]->StatFlush() != HXHIM_SUCCESS) {
            return HXHIM_ERROR;
        }
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhimStatFlush
 * Flushes the MDHIM statistics
 * Mainly needed for BGetOp
 *
 * @param hx the HXHIM session
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimStatFlush(hxhim_t *hx) {
    return hxhim::StatFlush(hx);
}

/**
 * FlushPuts
 * Flushes all queued PUTs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session
 * @return Pointer to return value wrapper
 */
hxhim::Results *hxhim::FlushPuts(hxhim_t *hx) {
    if (!hx || !hx->p) {
        return nullptr;
    }

    hxhim::Unsent<hxhim::PutData> &unsent = hx->p->puts;
    std::unique_lock<std::mutex> lock(unsent.mutex);
    unsent.force = true;
    unsent.start_processing.notify_all();

    // wait for flush to complete
    while (hx->p->running && unsent.force) {
        unsent.done_processing.wait(lock, [&](){ return !hx->p->running || !unsent.force; });
    }

    // retrieve all results
    std::unique_lock<std::mutex> results_lock(hx->p->async_put.mutex);
    hxhim::Results *res = hx->p->async_put.results;
    hx->p->async_put.results = new hxhim::Results();

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
    return hxhim_results_init(hxhim::FlushPuts(hx));
}

/**
 * get_core
 * The core set of function calls that are needed to send Gets to databases and receive responses
 *
 * @param hx        the HXHIM session
 * @param curr      the current set of data to process
 * @param count     the number of elements in this set of data
 * @param local     the buffer for local data  (not a local variable in order to avoid allocations)
 * @param remote    the buffer for remote data (not a local variable in order to avoid allocations)
 * @return
 */
static hxhim::Results *get_core(hxhim_t *hx,
                                hxhim::GetData *curr,
                                const std::size_t count,
                                Transport::Request::BGet *local,
                                Transport::Request::BGet **remote) {
    // reset buffers without deallocating (BGet::clean should be false)
    local->count = 0;
    for(std::size_t i = 0; i < hx->mpi.size; i++) {
        remote[i]->count = 0;
    }

    // move the data into the appropriate buffers
    for(std::size_t i = 0; i < count; i++) {
        hxhim::shuffle::Get(hx, count,
                            curr->subjects[i], curr->subject_lens[i],
                            curr->predicates[i], curr->predicate_lens[i],
                            curr->object_types[i],
                            local, remote);
    }

    hxhim::Results *res = new hxhim::Results();

    // GET the batch
    if (hx->mpi.size > 1) {
        Transport::Response::BGet *responses = hx->p->transport->BGet(hx->mpi.size, remote);
        for(Transport::Response::BGet *curr = responses; curr; curr = curr->next) {
            for(std::size_t i = 0; i < curr->count; i++) {
                res->Add(new hxhim::Results::Get(curr, i));
            }
        }
        delete responses;
    }

    if (local->count) {
        Transport::Response::BGet *responses = local_client_bget(hx, local);
        for(Transport::Response::BGet *curr = responses; curr; curr = curr->next) {
            for(std::size_t i = 0; i < curr->count; i++) {
                res->Add(new hxhim::Results::Get(curr, i));
            }
        }
        delete responses;
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
    if (!hx || !hx->p) {
        return nullptr;
    }

    hxhim::Unsent<hxhim::GetData> &gets = hx->p->gets;
    std::lock_guard<std::mutex> lock(gets.mutex);

    hxhim::GetData *curr = gets.head;
    if (!curr) {
        return HXHIM_SUCCESS;
    }

    Transport::Request::BGet local(HXHIM_MAX_BULK_GET_OPS);
    local.src = hx->mpi.rank;
    local.dst = hx->mpi.rank;

    Transport::Request::BGet **remote = new Transport::Request::BGet *[hx->mpi.size](); // list of destination servers (not databases) and messages to those destinations
    for(std::size_t i = 0; i < hx->mpi.size; i++) {
        remote[i] = new Transport::Request::BGet(HXHIM_MAX_BULK_GET_OPS);
    }

    // write complete batches
    hxhim::Results *res = new hxhim::Results();
    while (curr->next) {
        hxhim::Results *ret = get_core(hx, curr, HXHIM_MAX_BULK_GET_OPS, &local, remote);
        res->Append(ret);
        delete ret;

        // go to the next batch
        hxhim::GetData *next = curr->next;
        delete curr;
        curr = next;
    }

    hxhim::Results *ret = get_core(hx, curr, gets.last_count, &local, remote);
    res->Append(ret);
    delete ret;

    // delete the last batch
    delete curr;

    for(std::size_t i = 0; i < hx->mpi.size; i++) {
        delete remote[i];
    }
    delete [] remote;

    gets.head = gets.tail = nullptr;

    return res;
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
    return hxhim_results_init(hxhim::FlushGets(hx));
}

/**
 * getop_core
 * The core set of function calls that are needed to send GetOps to databases and receive responses
 *
 * @param hx        the HXHIM session
 * @param curr      the current set of data to process
 * @param count     the number of elements in this set of data
 * @param local     the buffer for local data  (not a local variable in order to avoid allocations)
 * @param remote    the buffer for remote data (not a local variable in order to avoid allocations)
 * @return
 */
static hxhim::Results *getop_core(hxhim_t *hx,
                                  hxhim::GetOpData *curr,
                                  const std::size_t count,
                                  Transport::Request::BGetOp *local,
                                  Transport::Request::BGetOp **remote) {
    // reset buffers without deallocating (BGetOp::clean should be false)
    local->count = 0;
    for(std::size_t i = 0; i < hx->mpi.size; i++) {
        remote[i]->count = 0;
    }

    // move the data into the appropriate buffers
    for(std::size_t i = 0; i < count; i++) {
        hxhim::shuffle::GetOp(hx, count,
                              curr->subjects[i], curr->subject_lens[i],
                              curr->predicates[i], curr->predicate_lens[i],
                              curr->object_types[i],
                              curr->num_recs[i],
                              curr->ops[i],
                              local, remote);
    }

    hxhim::Results *res = new hxhim::Results();

    // GET the batch
    if (hx->mpi.size > 1) {
        Transport::Response::BGetOp *responses = hx->p->transport->BGetOp(hx->mpi.size, remote);
        for(Transport::Response::BGetOp *curr = responses; curr; curr = curr->next) {
            for(std::size_t i = 0; i < curr->count; i++) {
                res->Add(new hxhim::Results::Get(curr, i));
            }
        }
        delete responses;
    }

    if (local->count) {
        Transport::Response::BGetOp *responses = local_client_bget_op(hx, local);
        for(Transport::Response::BGetOp *curr = responses; curr; curr = curr->next) {
            for(std::size_t i = 0; i < curr->count; i++) {
                res->Add(new hxhim::Results::Get(curr, i));
            }
        }
        delete responses;
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
    if (!hx || !hx->p) {
        return nullptr;
    }

    hxhim::Unsent<hxhim::GetOpData> &getops = hx->p->getops;
    std::lock_guard<std::mutex> lock(getops.mutex);

    hxhim::GetOpData *curr = getops.head;
    if (!curr) {
        return HXHIM_SUCCESS;
    }

    Transport::Request::BGetOp local(HXHIM_MAX_BULK_GET_OPS);
    local.src = hx->mpi.rank;
    local.dst = hx->mpi.rank;

    Transport::Request::BGetOp **remote = new Transport::Request::BGetOp *[hx->mpi.size](); // list of destination servers (not databases) and messages to those destinations
    for(std::size_t i = 0; i < hx->mpi.size; i++) {
        remote[i] = new Transport::Request::BGetOp(HXHIM_MAX_BULK_GET_OPS);
        remote[i]->src = hx->mpi.rank;
        remote[i]->dst = i;
    }

    hxhim::Results *res = new hxhim::Results();

    // write complete batches
    while (curr->next) {
        hxhim::Results *ret = getop_core(hx, curr, HXHIM_MAX_BULK_GET_OPS, &local, remote);
        res->Append(ret);
        delete ret;

        // go to the next batch
        hxhim::GetOpData *next = curr->next;
        delete curr;
        curr = next;
    }

    // write final (possibly incomplete) batch
    hxhim::Results *ret = getop_core(hx, curr, getops.last_count, &local, remote);
    res->Append(ret);
    delete ret;

    // delete the last batch
    delete curr;

    for(std::size_t i = 0; i < hx->mpi.size; i++) {
        delete remote[i];
    }
    delete [] remote;

    getops.head = getops.tail = nullptr;

    return res;
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
    return hxhim_results_init(hxhim::FlushGetOps(hx));
}

/**
 * delete_core
 * The core set of function calls that are needed to send Deletes to databases and receive responses
 *
 * @param hx        the HXHIM session
 * @param curr      the current set of data to process
 * @param count     the number of elements in this set of data
 * @param local     the buffer for local data  (not a local variable in order to avoid allocations)
 * @param remote    the buffer for remote data (not a local variable in order to avoid allocations)
 * @return
 */
static hxhim::Results *delete_core(hxhim_t *hx,
                                   hxhim::DeleteData *curr,
                                   const std::size_t count,
                                   Transport::Request::BDelete *local,
                                   Transport::Request::BDelete **remote) {
    // reset buffers without deallocating (BDelete::clean should be false)
    local->count = 0;
    for(std::size_t i = 0; i < hx->mpi.size; i++) {
        remote[i]->count = 0;
    }

    // move the data into the appropriate buffers
    for(std::size_t i = 0; i < count; i++) {
        hxhim::shuffle::Delete(hx, count,
                               curr->subjects[i], curr->subject_lens[i],
                               curr->predicates[i], curr->predicate_lens[i],
                               local, remote);
    }

    hxhim::Results *res = new hxhim::Results();

    // DELETE the batch
    if (hx->mpi.size > 1) {
        Transport::Response::BDelete *responses = hx->p->transport->BDelete(hx->mpi.size, remote);
        for(Transport::Response::BDelete *curr = responses; curr; curr = curr->next) {
            for(std::size_t i = 0; i < curr->count; i++) {
                res->Add(new hxhim::Results::Delete(curr, i));
            }
        }
        delete responses;
    }

    if (local->count) {
        Transport::Response::BDelete *responses = local_client_bdelete(hx, local);
        for(Transport::Response::BDelete *curr = responses; curr; curr = curr->next) {
            for(std::size_t i = 0; i < curr->count; i++) {
                res->Add(new hxhim::Results::Delete(curr, i));
            }
        }
        delete responses;
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
    if (!hx || !hx->p) {
        return nullptr;
    }

    hxhim::Unsent<hxhim::DeleteData> &dels = hx->p->deletes;
    std::lock_guard<std::mutex> lock(dels.mutex);

    hxhim::DeleteData *curr = dels.head;
    if (!curr) {
        return HXHIM_SUCCESS;
    }

    Transport::Request::BDelete local(HXHIM_MAX_BULK_GET_OPS);
    local.src = hx->mpi.rank;
    local.dst = hx->mpi.rank;

    Transport::Request::BDelete **remote = new Transport::Request::BDelete *[hx->mpi.size](); // list of destination servers (not databases) and messages to those destinations
    for(std::size_t i = 0; i < hx->mpi.size; i++) {
        remote[i] = new Transport::Request::BDelete(HXHIM_MAX_BULK_GET_OPS);
        remote[i]->src = hx->mpi.rank;
        remote[i]->dst = i;
    }

    hxhim::Results *res = new hxhim::Results();

    // write complete batches
    while (curr->next) {
        // move the data into the appropriate buffers
        res->Append(delete_core(hx, curr, HXHIM_MAX_BULK_DEL_OPS, &local, remote));

        // go to the next batch
        hxhim::DeleteData *next = curr->next;
        delete curr;
        curr = next;
    }

    // write final (possibly incomplete) batch
    // move the data into the appropriate buffers
    res->Append(delete_core(hx, curr, dels.last_count, &local, remote));

    // delete the last batch
    delete curr;

    for(std::size_t i = 0; i < hx->mpi.size; i++) {
        delete remote[i];
    }
    delete [] remote;

    dels.head = dels.tail = nullptr;

    return res;
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
    return hxhim_results_init(hxhim::FlushDeletes(hx));
}

/**
 * Flush
 *     1. Do all PUTs
 *     2. Do all GETs
 *     3. Do all GET_OPs
 *     4. Do all DELs
 *
 * @param hx
 * @return An array of results (3 values)
 */
hxhim::Results *hxhim::Flush(hxhim_t *hx) {
    hxhim::Results *res    = new hxhim::Results();
    hxhim::Results *puts   = FlushPuts(hx);
    hxhim::Results *gets   = FlushGets(hx);
    hxhim::Results *getops = FlushGetOps(hx);
    hxhim::Results *dels   = FlushDeletes(hx);

    res->Append(puts);   delete puts;
    res->Append(gets);   delete gets;
    res->Append(getops); delete getops;
    res->Append(dels);   delete dels;

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
    return hxhim_results_init(hxhim::Flush(hx));
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
    if (!hx || !hx->p) {
        return HXHIM_ERROR;
    }

    hxhim::Unsent<hxhim::PutData> &puts = hx->p->puts;
    std::lock_guard<std::mutex> lock(puts.mutex);

    // no previous batch
    if (!puts.tail) {
        puts.head       = new hxhim::PutData();
        puts.tail       = puts.head;
        puts.last_count = 0;
    }

    // filled the current batch
    if (puts.last_count == HXHIM_MAX_BULK_PUT_OPS) {
        hxhim::PutData *next = new hxhim::PutData();
        next->prev      = puts.tail;
        puts.tail->next = next;
        puts.tail       = next;
        puts.last_count = 0;
        puts.full_batches++;
        puts.start_processing.notify_one();
    }

    std::size_t &i = puts.last_count;

    puts.tail->subjects[i] = subject;
    puts.tail->subject_lens[i] = subject_len;

    puts.tail->predicates[i] = predicate;
    puts.tail->predicate_lens[i] = predicate_len;

    puts.tail->object_types[i] = object_type;
    puts.tail->objects[i] = object;
    puts.tail->object_lens[i] = object_len;

    i++;

    return HXHIM_SUCCESS;
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
    if (!hx || !hx->p) {
        return HXHIM_ERROR;
    }

    hxhim::Unsent<hxhim::GetData> &gets = hx->p->gets;
    std::lock_guard<std::mutex> lock(gets.mutex);

    // no previous batch
    if (!gets.tail) {
        gets.head       = new hxhim::GetData();
        gets.tail       = gets.head;
        gets.last_count = 0;
    }

    // filled the current batch
    if (gets.last_count == HXHIM_MAX_BULK_GET_OPS) {
        gets.tail->next = new hxhim::GetData();
        gets.tail       = gets.tail->next;
        gets.last_count = 0;
        gets.full_batches++;
    }

    std::size_t &i = gets.last_count;

    gets.tail->subjects[i] = subject;
    gets.tail->subject_lens[i] = subject_len;

    gets.tail->predicates[i] = predicate;
    gets.tail->predicate_lens[i] = predicate_len;

    gets.tail->object_types[i] = object_type;

    i++;

    return HXHIM_SUCCESS;
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
    if (!hx || !hx->p) {
        return HXHIM_ERROR;
    }

    hxhim::Unsent<hxhim::DeleteData> &dels = hx->p->deletes;
    std::lock_guard<std::mutex> lock(dels.mutex);

    // no previous batch
    if (!dels.tail) {
        dels.head       = new hxhim::DeleteData();
        dels.tail       = dels.head;
        dels.last_count = 0;
    }

    // filled the current batch
    if (dels.last_count == HXHIM_MAX_BULK_DEL_OPS) {
        dels.tail->next = new hxhim::DeleteData();
        dels.tail       = dels.tail->next;
        dels.last_count = 0;
        dels.full_batches++;
    }

    std::size_t &i = dels.last_count;

    dels.tail->subjects[i] = subject;
    dels.tail->subject_lens[i] = subject_len;

    dels.tail->predicates[i] = predicate;
    dels.tail->predicate_lens[i] = predicate_len;

    i++;

    return HXHIM_SUCCESS;
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
    if (!hx         || !hx->p          ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens ||
        !objects    || !object_lens) {
        return HXHIM_ERROR;
    }

    if (count) {
        hxhim::Unsent<hxhim::PutData> &puts = hx->p->puts;
        std::lock_guard<std::mutex> lock(puts.mutex);

        // no previous batch
        if (!puts.tail) {
            puts.head       = new hxhim::PutData();
            puts.tail       = puts.head;
            puts.last_count = 0;
        }

        for(std::size_t c = 0; c < count; c++) {
            // filled the current batch
            if (puts.last_count == HXHIM_MAX_BULK_PUT_OPS) {
                hxhim::PutData *next = new hxhim::PutData();
                next->prev      = puts.tail;
                puts.tail->next = next;
                puts.tail       = next;
                puts.last_count = 0;
                puts.full_batches++;
                puts.start_processing.notify_one();
            }

            std::size_t &i = puts.last_count;

            puts.tail->subjects[i] = subjects[c];
            puts.tail->subject_lens[i] = subject_lens[c];

            puts.tail->predicates[i] = predicates[c];
            puts.tail->predicate_lens[i] = predicate_lens[c];

            puts.tail->object_types[i] = object_types[c];
            puts.tail->objects[i] = objects[c];
            puts.tail->object_lens[i] = object_lens[c];

            i++;
        }
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
 * @param hx            the HXHIM session
 * @param hx            the HXHIM session
 * @param subjects      the subjects to get
 * @param subject_lens  the lengths of the subjects to get
 * @param prediates     the prediates to get
 * @param prediate_lens the lengths of the prediates to get
 * @param object_types  the types of the objects
 * @param count         the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BGet(hxhim_t *hx,
                void **subjects, std::size_t *subject_lens,
                void **predicates, std::size_t *predicate_lens,
                hxhim_type_t *object_types,
                std::size_t count) {
    if (!hx         || !hx->p          ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens) {
        return HXHIM_ERROR;
    }

    if (count) {
        hxhim::Unsent<hxhim::GetData> &gets = hx->p->gets;
        std::lock_guard<std::mutex> lock(gets.mutex);

        // no previous batch
        if (!gets.tail) {
            gets.head       = new hxhim::GetData();
            gets.tail       = gets.head;
            gets.last_count = 0;
        }

        for(std::size_t c = 0; c < count; c++) {
            // filled the current batch
            if (gets.last_count == HXHIM_MAX_BULK_GET_OPS) {
                gets.tail->next = new hxhim::GetData();
                gets.tail       = gets.tail->next;
                gets.last_count = 0;
                gets.full_batches++;
            }

            std::size_t &i = gets.last_count;

            gets.tail->subjects[i] = subjects[c];
            gets.tail->subject_lens[i] = subject_lens[c];

            gets.tail->predicates[i] = predicates[c];
            gets.tail->predicate_lens[i] = predicate_lens[c];

            gets.tail->object_types[i] = object_types[c];

            i++;
        }
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhimBGet
 * Add a BGET into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to get
 * @param subject_lens  the lengths of the subjects to get
 * @param prediates     the prediates to get
 * @param prediate_lens the lengths of the prediates to get
 * @param object_types  the types of the objects
 * @param count         the number of inputs
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
    if (!hx      || !hx->p       ||
        !subject || !subject_len) {
        return HXHIM_ERROR;
    }

    hxhim::Unsent<hxhim::GetOpData> &getops = hx->p->getops;
    std::lock_guard<std::mutex> lock(getops.mutex);

    // no previous batch
    if (!getops.tail) {
        getops.head       = new hxhim::GetOpData();
        getops.tail       = getops.head;
        getops.last_count = 0;
    }

    // filled the current batch
    if (getops.last_count == HXHIM_MAX_BULK_GET_OPS) {
        getops.tail->next = new hxhim::GetOpData();
        getops.tail       = getops.tail->next;
        getops.last_count = 0;
        getops.full_batches++;
    }

    std::size_t &i = getops.last_count;

    getops.tail->subjects[i] = subject;
    getops.tail->subject_lens[i] = subject_len;

    getops.tail->predicates[i] = predicate;
    getops.tail->predicate_lens[i] = predicate_len;

    getops.tail->object_types[i] = object_type;

    getops.tail->num_recs[i] = num_records;
    getops.tail->ops[i] = op;

    i++;

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
    if (!hx         || !hx->p          ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens) {
        return HXHIM_ERROR;
    }

    if (count) {
        hxhim::Unsent<hxhim::DeleteData> &dels = hx->p->deletes;
        std::lock_guard<std::mutex> lock(dels.mutex);

        // no previous batch
        if (!dels.tail) {
            dels.head     = new hxhim::DeleteData();
            dels.tail     = dels.head;
            dels.last_count = 0;
        }

        for(std::size_t c = 0; c < count; c++) {
            // filled the current batch
            if (dels.last_count == HXHIM_MAX_BULK_DEL_OPS) {
                dels.tail->next = new hxhim::DeleteData();
                dels.tail       = dels.tail->next;
                dels.last_count = 0;
                dels.full_batches++;
            }

            std::size_t &i = dels.last_count;

            dels.tail->subjects[i] = subjects[c];
            dels.tail->subject_lens[i] = subject_lens[c];

            dels.tail->predicates[i] = predicates[c];
            dels.tail->predicate_lens[i] = predicate_lens[c];

            i++;
        }
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
 * @param rank           the rank that is collecting the data
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
int hxhim::GetStats(hxhim_t *hx, const int rank,
                    const bool get_put_times, long double *put_times,
                    const bool get_num_puts, std::size_t *num_puts,
                    const bool get_get_times, long double *get_times,
                    const bool get_num_gets, std::size_t *num_gets) {
    return hx->p->databases[0]->GetStats(rank,
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
 * @param rank           the rank that is collecting the data
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
int hxhimGetStats(hxhim_t *hx, const int rank,
                  const int get_put_times, long double *put_times,
                  const int get_num_puts, std::size_t *num_puts,
                  const int get_get_times, long double *get_times,
                  const int get_num_gets, std::size_t *num_gets) {
    return hxhim::GetStats(hx, rank,
                           get_put_times, put_times,
                           get_num_puts, num_puts,
                           get_get_times, get_times,
                           get_num_gets, num_gets);
}

/**
 * GetHistogram
 * Get access to the histogram
 *
 * @param hx            the HXHIM session
 * @param histogram     variable to place the histogram pointer into
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::GetHistogram(hxhim_t *hx, Histogram::Histogram **histogram) {
    if (!hx || !hx->p || !histogram) {
        return HXHIM_ERROR;
    }

    return HXHIM_ERROR;
}

/**
 * hxhimgetHistogram
 * C version of hxhim::GetHistogram
 * Converts a Histogram::Histogram pointer into a histogram_t
 *
 * @param hx            the HXHIM session
 * @param histogram     variable to place the histogram pointer into
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimGetHistogram(hxhim_t *hx, histogram_t *histogram) {
    return hxhim::GetHistogram(hx, &histogram->histogram);
}
