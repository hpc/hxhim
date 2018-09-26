#include <algorithm>
#include <cfloat>
#include <cmath>

#include "datastore/datastores.hpp"
#include "hxhim/MaxSize.hpp"
#include "hxhim/config.hpp"
#include "hxhim/local_client.hpp"
#include "hxhim/options_private.hpp"
#include "hxhim/private.hpp"
#include "hxhim/shuffle.hpp"
#include "transport/transports.hpp"
#include "utils/FixedBufferPool.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

/**
 * clean
 * Convenience function for cleaning up caches
 *
 * @param hx    the HXHIM instance
 * @tparam node one node of the cache queue
 */
template <typename Data, typename = std::is_base_of<hxhim::SubjectPredicate, Data> >
void clean(hxhim_t *hx, Data *node) {
    while (node) {
        Data *next = node->next;
        hx->p->memory_pools.bulks->release(node);
        node = next;
    }
}

hxhim_private::hxhim_private()
    : bootstrap(),
      running(false),
      put_multiplier(1),
      max_bulk_ops(),
      queues(),
      datastore(),
      async_put(),
      hash(),
      transport(nullptr),
      range_server_destroy(nullptr),
      memory_pools()
{
    memset(&max_bulk_ops, 0, sizeof(max_bulk_ops));

    async_put.max_queued = 0;
    async_put.results = nullptr;
}

/**
 * put_core
 * The core functionality for putting a single batch of SPO triples into the datastore
 *
 * @param hx      the HXHIM context
 * @param head    the head of the list of SPO triple batches to send
 * @param count   the number of triples in each batch
 * @return Pointer to return value wrapper
 */
static hxhim::Results *put_core(hxhim_t *hx, hxhim::PutData *head, const std::size_t count) {
    mlog(HXHIM_CLIENT_DBG, "Start put_core");

    if (!count) {
        return nullptr;
    }

    // total number of triples that will be PUT
    const std::size_t total = HXHIM_PUT_MULTIPLER * count;

    Transport::Request::BPut local(hx->p->memory_pools.arrays, hx->p->memory_pools.buffers, hx->p->max_bulk_ops.puts);
    local.src = hx->p->bootstrap.rank;
    local.dst = hx->p->bootstrap.rank;
    local.count = 0;

    // list of destination servers (not datastores) and messages to those destinations
    Transport::Request::BPut **remote = hx->p->memory_pools.arrays->acquire_array<Transport::Request::BPut *>(hx->p->bootstrap.size);
    for(int i = 0; i < hx->p->bootstrap.size; i++) {
        remote[i] = hx->p->memory_pools.requests->acquire<Transport::Request::BPut>(hx->p->memory_pools.arrays, hx->p->memory_pools.buffers, hx->p->max_bulk_ops.puts);
        remote[i]->src = hx->p->bootstrap.rank;
        remote[i]->dst = i;
        remote[i]->count = 0;
    }

    // zero out local message in remote messages
    hx->p->memory_pools.requests->release(remote[hx->p->bootstrap.rank]);
    remote[hx->p->bootstrap.rank] = nullptr;

    // place the input into destination buckets
    for(std::size_t i = 0; i < count; i++) {
        // alias the values
        void *subject = head->subjects[i];
        std::size_t subject_len = head->subject_lens[i];

        void *predicate = head->predicates[i];
        std::size_t predicate_len = head->predicate_lens[i];

        hxhim_type_t object_type = head->object_types[i];
        void *object = head->objects[i];
        std::size_t object_len = head->object_lens[i];

        // SP -> O
        hxhim::shuffle::Put(hx, total,
                            subject, subject_len,
                            predicate, predicate_len,
                            object_type, object, object_len,
                            &local, remote,
                            hx->p->memory_pools.requests);

        // // SO -> P
        // hxhim::shuffle::Put(hx, total,
        //                     subject, subject_len,
        //                     object, object_len,
        //                     HXHIM_BYTE_TYPE, predicate, predicate_len,
        //                     &local, &remote);

        // // PO -> S
        // hxhim::shuffle::Put(hx, total,
        //                     predicate, predicate_len,
        //                     object, object_len,
        //                     HXHIM_BYTE_TYPE, subject, subject_len,
        //                     &local, &remote);

        // // PS -> O
        // hxhim::shuffle::Put(hx, total,
        //                     predicate, predicate_len,
        //                     subject, subject_len,
        //                     object_type, object, object_len,
        //                     &local, &remote);
    }

    mlog(HXHIM_CLIENT_DBG, "Shuffled PUTs");

    hxhim::Results *res = hx->p->memory_pools.results->acquire<hxhim::Results>(hx);

    // PUT the batch
    if (hx->p->bootstrap.size > 1) {
        mlog(HXHIM_CLIENT_DBG, "Start remote PUTs");
        Transport::Response::BPut *responses = hx->p->transport->BPut(hx->p->bootstrap.size, remote);
        for(Transport::Response::BPut *curr = responses; curr; curr = Transport::next(curr, hx->p->memory_pools.responses)) {
            for(std::size_t i = 0; i < curr->count; i++) {
                res->Add(hx->p->memory_pools.result->acquire<hxhim::Results::Put>(hx, curr, i));
            }
        }
        mlog(HXHIM_CLIENT_DBG, "Completed remote PUTs");
    }

    if (local.count) {
        mlog(HXHIM_CLIENT_DBG, "Start local PUTs");
        Transport::Response::BPut *responses = local_client_bput(hx, &local);
        for(Transport::Response::BPut *curr = responses; curr; curr = Transport::next(curr, hx->p->memory_pools.responses)) {
            for(std::size_t i = 0; i < curr->count; i++) {
                res->Add(hx->p->memory_pools.result->acquire<hxhim::Results::Put>(hx, curr, i));
            }
        }
        mlog(HXHIM_CLIENT_DBG, "Completed local PUTs");
    }

    // cleanup
    for(int i = 0; i < hx->p->bootstrap.size; i++) {
        hx->p->memory_pools.requests->release(remote[i]);
    }
    hx->p->memory_pools.arrays->release_array(remote, hx->p->bootstrap.size);

    mlog(HXHIM_CLIENT_DBG, "Completed put_core");

    return res;
}

/**
 * backgroundPUT
 * The thread that runs when the number of full batches crosses the queued bputs watermark
 *
 * @param args   hx typecast to void *
 */
static void backgroundPUT(void *args) {
    hxhim_t *hx = (hxhim_t *) args;
    if (!hx || !hx->p) {
        return;
    }

    mlog(HXHIM_CLIENT_DBG, "Started background PUT thread");

    while (hx->p->running) {
        hxhim::PutData *head = nullptr;    // the first batch of PUTs to process

        bool force = false;                // whether or not FlushPuts was called
        hxhim::PutData *last = nullptr;    // pointer to the last batch of PUTs; only valid if force is true
        std::size_t last_count = 0;        // number of SPO triples in the last batch

        // hold unsent.mutex just long enough to move queued PUTs to send queue
        {
            hxhim::Unsent<hxhim::PutData> &unsent = hx->p->queues.puts;
            std::unique_lock<std::mutex> lock(unsent.mutex);
            while (hx->p->running && (unsent.full_batches < hx->p->async_put.max_queued) && !unsent.force) {
                mlog(HXHIM_CLIENT_DBG, "Waiting for %zu bulk puts (currently have %zu)", hx->p->async_put.max_queued, unsent.full_batches);
                unsent.start_processing.wait(lock, [&]() -> bool { return !hx->p->running || (unsent.full_batches >= hx->p->async_put.max_queued) || unsent.force; });
            }

            mlog(HXHIM_CLIENT_DBG, "Moving %zu queued bulk PUTs into send queue for processing %zu %zu %d", unsent.full_batches, unsent.full_batches, hx->p->async_put.max_queued, unsent.force);

            // record whether or not this loop was forced, since the lock is not held
            force = unsent.force;

            if (hx->p->running) {
                unsent.full_batches = 0;

                // nothing to do
                if (!unsent.head) {
                    if (force) {
                        unsent.force = false;
                        unsent.done_processing.notify_all();
                    }
                    continue;
                }

                if (force) {
                    // current batch is not the last one
                    if (unsent.head->next) {
                        head = unsent.head;
                        last = unsent.tail;

                        // disconnect the tail from the previous batches
                        last->prev->next = nullptr;
                    }
                    // current batch is the last one
                    else {
                        head = nullptr;
                        last = unsent.head;
                    }

                    // keep track of the size of the last batch
                    last_count = unsent.last_count;
                    unsent.last_count = 0;
                    unsent.force = false;

                    // remove all of the PUTs from the queue
                    unsent.head = nullptr;
                    unsent.tail = nullptr;
                }
                // not forced
                else {
                    // current batch is not the last one
                    if (unsent.head->next) {
                        head = unsent.head;
                        unsent.head = unsent.tail;

                        // disconnect the tail from the previous batches
                        unsent.tail->prev->next = nullptr;
                    }
                    // current batch is the last one
                    else {
                        continue;
                    }
                }
            }
        }

        // process the queued PUTs
        mlog(HXHIM_CLIENT_DBG, "Processing queued PUTs");
        while (hx->p->running && head) {
            // process the batch and save the results
            hxhim::Results *res = put_core(hx, head, hx->p->max_bulk_ops.puts);

            // go to the next batch
            hxhim::PutData *next = head->next;
            hx->p->memory_pools.bulks->release(head);
            head = next;

            {
                std::unique_lock<std::mutex> lock(hx->p->async_put.mutex);
                hx->p->async_put.results->Append(res);
            }

            hx->p->memory_pools.results->release(res);
        }
        mlog(HXHIM_CLIENT_DBG, "Done processing queued PUTs");

        // if this flush was forced, notify FlushPuts
        if (force) {
            mlog(HXHIM_CLIENT_DBG, "Forcing flush in backgroud PUT thread");

            if (hx->p->running) {
                mlog(HXHIM_CLIENT_DBG, "Force processing queued PUTs");

                // process the batch
                hxhim::Results *res = put_core(hx, last, last_count);

                hx->p->memory_pools.bulks->release(last);
                last = nullptr;

                {
                    std::unique_lock<std::mutex> lock(hx->p->async_put.mutex);
                    hx->p->async_put.results->Append(res);
                }

                hx->p->memory_pools.results->release(res);

                mlog(HXHIM_CLIENT_DBG, "Done force Processing queued PUTs");
            }

            hxhim::Unsent<hxhim::PutData> &unsent = hx->p->queues.puts;
            std::unique_lock<std::mutex> lock(unsent.mutex);
            unsent.done_processing.notify_all();
        }

        // clean up in case previous loop stopped early
        clean(hx, head);
        hx->p->memory_pools.bulks->release(last);
    }

    mlog(HXHIM_CLIENT_DBG, "Background PUT thread stopping");
}

/**
 * valid
 * Checks if hx and opts are ready to be used
 *
 * @param hx   the HXHIM instance
 * @param opts the HXHIM options
 * @param true if ready, else false
 */
static bool valid(hxhim_t *hx, hxhim_options_t *opts) {
    return hx && hx->p && opts && opts->p;
}

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
    if (!valid(hx, opts)) {
        return HXHIM_ERROR;
    }

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
int hxhim::init::running(hxhim_t *hx, hxhim_options_t *opts) {
    if (!valid(hx, opts)) {
        return HXHIM_ERROR;
    }

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
    if (!valid(hx, opts)) {
        return HXHIM_ERROR;
    }

    // size of each set of queued messages
    hx->p->max_bulk_ops.max     = opts->p->bulk_op_size;
    hx->p->max_bulk_ops.puts    = opts->p->bulk_op_size / HXHIM_PUT_MULTIPLER;
    hx->p->max_bulk_ops.gets    = opts->p->bulk_op_size;
    hx->p->max_bulk_ops.getops  = opts->p->bulk_op_size;
    hx->p->max_bulk_ops.deletes = opts->p->bulk_op_size;

    // set up the memory pools
    if (!((hx->p->memory_pools.keys      = new FixedBufferPool(opts->p->keys.alloc_size,      opts->p->keys.regions,      opts->p->keys.name))      &&
          (hx->p->memory_pools.buffers   = new FixedBufferPool(opts->p->buffers.alloc_size,   opts->p->buffers.regions,   opts->p->buffers.name))   &&
          (hx->p->memory_pools.bulks     = new FixedBufferPool(opts->p->bulks.alloc_size,     opts->p->bulks.regions,     opts->p->bulks.name))     &&
          (hx->p->memory_pools.arrays    = new FixedBufferPool(opts->p->arrays.alloc_size,    opts->p->arrays.regions,    opts->p->arrays.name))    &&
          (hx->p->memory_pools.requests  = new FixedBufferPool(opts->p->requests.alloc_size,  opts->p->requests.regions,  opts->p->requests.name))  &&
          (hx->p->memory_pools.responses = new FixedBufferPool(opts->p->responses.alloc_size, opts->p->responses.regions, opts->p->responses.name)) &&
          (hx->p->memory_pools.result    = new FixedBufferPool(opts->p->result.alloc_size,    opts->p->result.regions,    opts->p->result.name))    &&
          (hx->p->memory_pools.results   = new FixedBufferPool(opts->p->results.alloc_size,   opts->p->results.regions,   opts->p->results.name)))) {
        mlog(HXHIM_CLIENT_ERR, "Could not preallocate all buffers");
        return HXHIM_ERROR;
    }

    mlog(HXHIM_CLIENT_INFO, "Preallocated %zu bytes of memory for HXHIM",
         hx->p->memory_pools.keys->size()      +
         hx->p->memory_pools.buffers->size()   +
         hx->p->memory_pools.bulks->size()     +
         hx->p->memory_pools.arrays->size()    +
         hx->p->memory_pools.requests->size()  +
         hx->p->memory_pools.responses->size() +
         hx->p->memory_pools.result->size()    +
         hx->p->memory_pools.results->size());
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
    if (!valid(hx, opts)) {
        return HXHIM_ERROR;
    }

    if (!(hx->p->datastore.count = opts->p->datastore_count)) {
        return HXHIM_ERROR;
    }

    if (!(hx->p->datastore.datastores = hx->p->memory_pools.arrays->acquire_array<hxhim::datastore::Datastore *>(hx->p->datastore.count))) {
        return HXHIM_ERROR;
    }

    for(std::size_t i = 0; i < hx->p->datastore.count; i++) {
        Histogram::Histogram *hist = new Histogram::Histogram(opts->p->histogram.first_n,
                                                              opts->p->histogram.gen,
                                                              opts->p->histogram.args);

        // Start the datastore
        switch (opts->p->datastore->type) {
            #if HXHIM_HAVE_LEVELDB
            case hxhim::datastore::LEVELDB:
                {
                    hxhim_leveldb_config_t *config = static_cast<hxhim_leveldb_config_t *>(opts->p->datastore);
                    hx->p->datastore.prefix = config->prefix;
                    hx->p->datastore.datastores[i] = new hxhim::datastore::leveldb(hx,
                                                                                   hxhim::datastore::get_id(hx, hx->p->bootstrap.rank, i),
                                                                                   hist,
                                                                                   hx->p->hash.name, config->create_if_missing);
                    mlog(HXHIM_CLIENT_INFO, "Initialized LevelDB in datastore[%zu]", i);
                }
                break;
            #endif
            case hxhim::datastore::IN_MEMORY:
                {
                    hx->p->datastore.prefix = "";
                    hx->p->datastore.datastores[i] = new hxhim::datastore::InMemory(hx,
                                                                                    hxhim::datastore::get_id(hx, hx->p->bootstrap.rank, i),
                                                                                    hist,
                                                                                    hx->p->hash.name);
                    mlog(HXHIM_CLIENT_INFO, "Initialized In-Memory in datastore[%zu]", i);
                }
                break;
            default:
                break;
        }

        if (!hx->p->datastore.datastores[i]) {
            return HXHIM_ERROR;
        }
    }

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

    if ((hx->p->datastore.count = opts->p->datastore_count) != 1) {
        return HXHIM_ERROR;
    }

    if (!(hx->p->datastore.datastores = hx->p->memory_pools.arrays->acquire_array<hxhim::datastore::Datastore *>(1))) {
        return HXHIM_ERROR;
    }

    Histogram::Histogram *hist = new Histogram::Histogram(opts->p->histogram.first_n,
                                                          opts->p->histogram.gen,
                                                          opts->p->histogram.args);

    // Start the datastore
    switch (opts->p->datastore->type) {
        #if HXHIM_HAVE_LEVELDB
        case hxhim::datastore::LEVELDB:
            hx->p->datastore.datastores[0] = new hxhim::datastore::leveldb(hx,
                                                                           hist,
                                                                           name);
            mlog(HXHIM_CLIENT_INFO, "Initialized single LevelDB datastore");
            break;
        #endif
        case hxhim::datastore::IN_MEMORY:
            hx->p->datastore.datastores[0] = new hxhim::datastore::InMemory(hx,
                                                                            0,
                                                                            hist,
                                                                            name);
            mlog(HXHIM_CLIENT_INFO, "Initialized single In-Memory datastore");
            break;
        default:
            break;
    }

    return hx->p->datastore.datastores[0]?HXHIM_SUCCESS:HXHIM_ERROR;
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
    if (!valid(hx, opts)) {
        return HXHIM_ERROR;
    }

    if (init::running(hx, opts) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    // Set number of bulk puts to queue up before sending
    hx->p->async_put.max_queued = opts->p->start_async_bput_at;

    // Set up queued PUT results list
    if (!(hx->p->async_put.results = hx->p->memory_pools.results->acquire<hxhim::Results>(hx))) {
        return HXHIM_ERROR;
    }

    // Start the background thread
    hx->p->async_put.thread = std::thread(backgroundPUT, hx);

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
    if (!valid(hx, opts)) {
        return HXHIM_ERROR;
    }

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
    if (!valid(hx, opts)) {
        return HXHIM_ERROR;
    }

    delete hx->p->transport;
    if (!(hx->p->transport = new Transport::Transport())) {
        return HXHIM_ERROR;
    }

    int ret = TRANSPORT_ERROR;

    switch (opts->p->transport->type) {
        case Transport::TRANSPORT_NULL:
            hx->p->range_server_destroy = nullptr;
            break;
        case Transport::TRANSPORT_MPI:
            ret = Transport::MPI::init(hx, opts);
            break;
        #if HXHIM_HAVE_THALLIUM
        case Transport::TRANSPORT_THALLIUM:
            ret = Transport::Thallium::init(hx, opts);
            break;
        #endif
        default:
            mlog(HXHIM_CLIENT_ERR, "Received bad transport type to initialize %d", opts->p->transport->type);
            break;
    }

    mlog(HXHIM_CLIENT_INFO, "Completed Transport Initialization");
    return (ret == TRANSPORT_SUCCESS)?HXHIM_SUCCESS:HXHIM_ERROR;
}

/**
 * valid
 * Checks if hx can be closed
 *
 * @param hx   the HXHIM instance
 * @param true if ready, else false
 */
static bool valid(hxhim_t *hx) {
    return hx && hx->p;
}

/**
 * bootstrap
 * Invalidates the MPI bootstrapping information
 *
 * @param hx   the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::destroy::bootstrap(hxhim_t *hx) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    hx->p->bootstrap.comm = MPI_COMM_NULL;
    hx->p->bootstrap.rank = -1;
    hx->p->bootstrap.size = -1;

    return HXHIM_SUCCESS;
}

/**
 * running
 * Sets the state to not running
 *
 * @param hx   the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::destroy::running(hxhim_t *hx) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    hx->p->running = false;
    return HXHIM_SUCCESS;
}

/**
 * memory
 * Removes the memory pools
 *
 * @param hx   the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::destroy::memory(hxhim_t *hx) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    delete hx->p->memory_pools.keys;
    delete hx->p->memory_pools.buffers;
    delete hx->p->memory_pools.bulks;
    delete hx->p->memory_pools.arrays;
    delete hx->p->memory_pools.requests;
    delete hx->p->memory_pools.responses;
    delete hx->p->memory_pools.result;
    delete hx->p->memory_pools.results;

    hx->p->memory_pools.keys      = nullptr;
    hx->p->memory_pools.buffers   = nullptr;
    hx->p->memory_pools.bulks     = nullptr;
    hx->p->memory_pools.arrays    = nullptr;
    hx->p->memory_pools.arrays    = nullptr;
    hx->p->memory_pools.requests  = nullptr;
    hx->p->memory_pools.responses = nullptr;
    hx->p->memory_pools.result    = nullptr;
    hx->p->memory_pools.results   = nullptr;

    return HXHIM_SUCCESS;
}

/**
 * transport
 * Cleans up the transport
 *
 * @param hx   the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::destroy::transport(hxhim_t *hx) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    if (hx->p->range_server_destroy) {
        hx->p->range_server_destroy();
    }

    if (hx->p->transport) {
        delete hx->p->transport;
        hx->p->transport = nullptr;
    }

    return HXHIM_SUCCESS;
}

/**
 * hash
 * Cleans up the hash function
 *
 * @param hx   the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::destroy::hash(hxhim_t *hx) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    hx->p->hash.func = nullptr;
    hx->p->hash.args = nullptr;

    return HXHIM_SUCCESS;
}

/**
 * async_put
 * Stops the background thread and cleans up the variables used by it
 *
 * @param hx   the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::destroy::async_put(hxhim_t *hx) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    // stop the thread
    destroy::running(hx);
    hx->p->queues.puts.start_processing.notify_all();
    hx->p->queues.puts.done_processing.notify_all();

    if (hx->p->async_put.thread.joinable()) {
        hx->p->async_put.thread.join();
    }

    // clear out unflushed work in the work queue
    clean(hx, hx->p->queues.puts.head);
    hx->p->queues.puts.head = nullptr;
    clean(hx, hx->p->queues.gets.head);
    hx->p->queues.gets.head = nullptr;
    clean(hx, hx->p->queues.getops.head);
    hx->p->queues.getops.head = nullptr;
    clean(hx, hx->p->queues.deletes.head);
    hx->p->queues.deletes.head = nullptr;

    // release unproceesed results from asynchronous PUTs
    {
        std::unique_lock<std::mutex>(hx->p->async_put.mutex);
        hx->p->memory_pools.results->release(hx->p->async_put.results);
        hx->p->async_put.results = nullptr;
    }

    return HXHIM_SUCCESS;
}

/**
 * datastore
 * Cleans up the datastore
 *
 * @param hx   the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::destroy::datastore(hxhim_t *hx) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    if (hx->p->datastore.datastores) {
        for(std::size_t i = 0; i < hx->p->datastore.count; i++) {
            if (hx->p->datastore.datastores[i]) {
                hx->p->datastore.datastores[i]->Close();
                delete hx->p->datastore.datastores[i];
                hx->p->datastore.datastores[i] = nullptr;
            }
        }

        hx->p->memory_pools.arrays->release_array(hx->p->datastore.datastores, hx->p->datastore.count);
        hx->p->datastore.datastores = nullptr;
    }

    return HXHIM_SUCCESS;
}
