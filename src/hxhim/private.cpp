#include <algorithm>
#include <cfloat>

#include "datastore/datastores.hpp"
#include "hxhim/config.hpp"
#include "hxhim/local_client.hpp"
#include "hxhim/options_private.hpp"
#include "hxhim/private.hpp"
#include "hxhim/shuffle.hpp"
#include "transport/backend/backends.hpp"
#include "transport/transport.hpp"
#include "utils/MemoryManager.hpp"
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
    // these should be configurable
    max_bulk_ops.max     = HXHIM_MAX_BULK_OPS;
    max_bulk_ops.puts    = max_bulk_ops.max / put_multiplier;
    max_bulk_ops.gets    = max_bulk_ops.max;
    max_bulk_ops.getops  = max_bulk_ops.max;
    max_bulk_ops.deletes = max_bulk_ops.max;

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
    if (!count) {
        return nullptr;
    }

    const std::size_t total = HXHIM_PUT_MULTIPLER * count;                                                           // total number of triples that will be PUT

    Transport::Request::BPut local(hxhim::GetArrayFBP(hx), hxhim::GetBufferFBP(hx), hx->p->max_bulk_ops.puts);
    local.src = hx->p->bootstrap.rank;
    local.dst = hx->p->bootstrap.rank;
    local.count = 0;

    Transport::Request::BPut **remote = hx->p->memory_pools.arrays->acquire_array<Transport::Request::BPut *>(hx->p->bootstrap.size); // list of destination servers (not datastores) and messages to those destinations
    for(std::size_t i = 0; i < hx->p->bootstrap.size; i++) {
        remote[i] = hx->p->memory_pools.requests->acquire<Transport::Request::BPut>(hxhim::GetArrayFBP(hx), hxhim::GetBufferFBP(hx), hx->p->max_bulk_ops.puts);
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

    hxhim::Results *res = hx->p->memory_pools.results->acquire<hxhim::Results>(hx);

    // PUT the batch
    if (hx->p->bootstrap.size > 1) {
        Transport::Response::BPut *responses = hx->p->transport->BPut(hx->p->bootstrap.size, remote);
        for(Transport::Response::BPut *curr = responses; curr; curr = curr->next) {
            for(std::size_t i = 0; i < curr->count; i++) {
                res->Add(hx->p->memory_pools.result->acquire<hxhim::Results::Put>(hx, curr, i));
            }
        }
        hx->p->memory_pools.responses->release(responses);
    }

    if (local.count) {
        Transport::Response::BPut *responses = local_client_bput(hx, &local);
        for(Transport::Response::BPut *curr = responses; curr; curr = curr->next) {
            for(std::size_t i = 0; i < curr->count; i++) {
                res->Add(hx->p->memory_pools.result->acquire<hxhim::Results::Put>(hx, curr, i));
            }
        }
        hx->p->memory_pools.responses->release(responses);
    }

    // cleanup
    for(std::size_t i = 0; i < hx->p->bootstrap.size; i++) {
        hx->p->memory_pools.requests->release(remote[i]);
    }
    hx->p->memory_pools.arrays->release_array(remote, hx->p->bootstrap.size);

    return res;
}

/**
 * backgroundPUT
 * The thread that runs when the number of full batches crosses the queued bputs watermark
 *
 * @param args   hx typecast to void *
 */
static void backgroundPUT(void *args) {
    mlog(HXHIM_CLIENT_INFO, "Started background PUT thread");

    hxhim_t *hx = (hxhim_t *) args;
    if (!hx || !hx->p) {
        return;
    }

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

            mlog(HXHIM_CLIENT_DBG, "Moving queued %zu PUTs into send queue for processing", unsent.full_batches);

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
            hxhim::Results *res = put_core(hx, head, HXHIM_MAX_BULK_PUT_OPS);

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

    mlog(HXHIM_CLIENT_INFO, "Background PUT thread stopping");
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
    if (!valid(hx, opts)) {
        return HXHIM_ERROR;
    }

    if (((hx->p->bootstrap.comm = opts->p->comm)                      == MPI_COMM_NULL) ||
        (MPI_Comm_rank(hx->p->bootstrap.comm, &hx->p->bootstrap.rank) != MPI_SUCCESS)   ||
        (MPI_Comm_size(hx->p->bootstrap.comm, &hx->p->bootstrap.size) != MPI_SUCCESS))   {
        return HXHIM_ERROR;
    }

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
 * Sets up the memory pools
 * TODO: make these configurable
 *
 * @param hx   the HXHIM instance
 * @param opts the HXHIM options
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::memory(hxhim_t *hx, hxhim_options_t *opts) {
    if (!valid(hx, opts)) {
        return HXHIM_ERROR;
    }

    if (!((hx->p->memory_pools.packed    = MemoryManager::FBP(opts->p->packed.alloc_size,    opts->p->packed.regions,    opts->p->packed.name.c_str()))    &&
          (hx->p->memory_pools.buffers   = MemoryManager::FBP(opts->p->buffers.alloc_size,   opts->p->buffers.regions,   opts->p->buffers.name.c_str()))   &&
          (hx->p->memory_pools.bulks     = MemoryManager::FBP(opts->p->bulks.alloc_size,     opts->p->bulks.regions,     opts->p->bulks.name.c_str()))     &&
          (hx->p->memory_pools.keys      = MemoryManager::FBP(opts->p->keys.alloc_size,      opts->p->keys.regions,      opts->p->keys.name.c_str()))      &&
          (hx->p->memory_pools.arrays    = MemoryManager::FBP(opts->p->arrays.alloc_size,    opts->p->arrays.regions,    opts->p->arrays.name.c_str()))    &&
          (hx->p->memory_pools.requests  = MemoryManager::FBP(opts->p->requests.alloc_size,  opts->p->requests.regions,  opts->p->requests.name.c_str()))  &&
          (hx->p->memory_pools.responses = MemoryManager::FBP(opts->p->responses.alloc_size, opts->p->responses.regions, opts->p->responses.name.c_str())) &&
          (hx->p->memory_pools.result    = MemoryManager::FBP(opts->p->result.alloc_size,    opts->p->result.regions,    opts->p->result.name.c_str()))    &&
          (hx->p->memory_pools.results   = MemoryManager::FBP(opts->p->results.alloc_size,   opts->p->results.regions,   opts->p->results.name.c_str())))) {
        mlog(HXHIM_CLIENT_CRIT, "Could not preallocate all buffers");
        return HXHIM_ERROR;
    }

    mlog(HXHIM_CLIENT_INFO, "Preallocated %zu bytes for HXHIM",
         hx->p->memory_pools.packed->size() +
         hx->p->memory_pools.buffers->size() +
         hx->p->memory_pools.bulks->size() +
         hx->p->memory_pools.keys->size() +
         hx->p->memory_pools.arrays->size() +
         hx->p->memory_pools.requests->size() +
         hx->p->memory_pools.responses->size() +
         hx->p->memory_pools.result->size() +
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
        // Start the datastore
        switch (opts->p->datastore->type) {
            #if HXHIM_HAVE_LEVELDB
            case HXHIM_DATASTORE_LEVELDB:
                {
                    hxhim_leveldb_config_t *config = static_cast<hxhim_leveldb_config_t *>(opts->p->datastore);
                    hx->p->datastore.datastores[i] = new hxhim::datastore::leveldb(hx,
                                                                                   config->id,
                                                                                   opts->p->histogram.first_n,
                                                                                   opts->p->histogram.gen,
                                                                                   opts->p->histogram.args,
                                                                                   config->path, config->create_if_missing);
                }
                break;
            #endif
            case HXHIM_DATASTORE_IN_MEMORY:
                {
                    hx->p->datastore.datastores[i] = new hxhim::datastore::InMemory(hx,
                                                                                    i,
                                                                                    opts->p->histogram.first_n,
                                                                                    opts->p->histogram.gen,
                                                                                    opts->p->histogram.args);
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

    // Start the datastore
    switch (opts->p->datastore->type) {
        #if HXHIM_HAVE_LEVELDB
        case HXHIM_DATASTORE_LEVELDB:
            hx->p->datastore.datastores[0] = new hxhim::datastore::leveldb(hx,
                                                                           opts->p->histogram.first_n,
                                                                           opts->p->histogram.gen,
                                                                           opts->p->histogram.args,
                                                                           name);
            break;
        #endif
        case HXHIM_DATASTORE_IN_MEMORY:
            hx->p->datastore.datastores[0] = new hxhim::datastore::InMemory(hx,
                                                                            0,
                                                                            opts->p->histogram.first_n,
                                                                            opts->p->histogram.gen,
                                                                            opts->p->histogram.args);
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
    if (!valid(hx, opts)) {
        return HXHIM_ERROR;
    }

    hx->p->hash.func = opts->p->hash;
    hx->p->hash.args = opts->p->hash_args;

    return HXHIM_SUCCESS;
}

static int init_transport_null(hxhim_t *hx, hxhim_options_t *opts) {
    mlog(HXHIM_CLIENT_DBG, "Starting NULL Transport Initialization");

    if (!valid(hx, opts)) {
        return HXHIM_ERROR;
    }

    hx->p->range_server_destroy = nullptr;

    mlog(HXHIM_CLIENT_DBG, "Completed NULL Transport Initialization");
    return TRANSPORT_SUCCESS;
}

/**
 * init_transport_mpi
 * Initializes MPI inside HXHIM
 *
 * @param hx             the HXHIM instance
 * @param opts           the HXHIM options
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
static int init_transport_mpi(hxhim_t *hx, hxhim_options_t *opts) {
    mlog(HXHIM_CLIENT_DBG, "Starting MPI Initialization");
    if (!valid(hx, opts) || !hx->p->transport) {
        return HXHIM_ERROR;
    }

    using namespace Transport::MPI;

    // Do not allow MPI_COMM_NULL
    if (hx->p->bootstrap.comm == MPI_COMM_NULL) {
        return TRANSPORT_ERROR;
    }

    Options *config = static_cast<Options *>(opts->p->transport);

    // give the range server access to the state
    RangeServer::init(hx, config->listeners);

    EndpointGroup *eg = new EndpointGroup(hx->p->bootstrap.comm,
                                          hx->p->running,
                                          hx->p->memory_pools.packed,
                                          hx->p->memory_pools.responses,
                                          hx->p->memory_pools.arrays,
                                          hx->p->memory_pools.buffers);
    if (!eg) {
        return TRANSPORT_ERROR;
    }

    // create mapping between unique IDs and ranks
    for(int i = 0; i < hx->p->bootstrap.size; i++) {
        // MPI ranks map 1:1 with the boostrap MPI rank
        hx->p->transport->AddEndpoint(i, new Endpoint(hx->p->bootstrap.comm, i,
                                                      hx->p->running,
                                                      hx->p->memory_pools.packed,
                                                      hx->p->memory_pools.responses,
                                                      hx->p->memory_pools.arrays,
                                                      hx->p->memory_pools.buffers));

        // if the rank was specified as part of the endpoint group, add the rank to the endpoint group
        if (opts->p->endpointgroup.find(i) != opts->p->endpointgroup.end()) {
            eg->AddID(i, i);
        }
    }

    // remove loopback endpoint
    hx->p->transport->RemoveEndpoint(hx->p->bootstrap.rank);

    hx->p->transport->SetEndpointGroup(eg);
    hx->p->range_server_destroy = RangeServer::destroy;

    mlog(HXHIM_CLIENT_DBG, "Completed MPI Initialization");
    return TRANSPORT_SUCCESS;
}

#if HXHIM_HAVE_THALLIUM

/**
 * init_transport_thallium
 * Initializes Thallium inside HXHIM
 *
 * @param hx   the HXHIM instance
 * @param opts the HXHIM options
 * @param TRANSPORT_SUCCESS or TRANSPORT_ERROR
 */
static int init_transport_thallium(hxhim_t *hx, hxhim_options_t *opts) {
    mlog(HXHIM_CLIENT_DBG, "Starting Thallium Initialization");
    if (!valid(hx, opts)) {
        return HXHIM_ERROR;
    }

    using namespace Transport::Thallium;

    Options *config = static_cast<Options *>(opts->p->transport);

    // create the engine (only 1 instance per process)
    Engine_t engine(new thallium::engine(config->module, THALLIUM_SERVER_MODE, true, -1),
                    [](thallium::engine *engine) {
                        engine->finalize();
                        delete engine;
                    });

    mlog(HXHIM_CLIENT_DBG, "Created Thallium engine %s", ((std::string) engine->self()).c_str());

    // give the range server access to the mdhim_t data
    RangeServer::init(hx);

    // create client to range server RPC
    RPC_t rpc(new thallium::remote_procedure(engine->define(RangeServer::CLIENT_TO_RANGE_SERVER_NAME,
                                                            RangeServer::process)));

    mlog(HXHIM_CLIENT_DBG, "Created Thallium RPC");

    // wait for every engine to start up
    MPI_Barrier(hx->p->bootstrap.comm);

    // get a mapping of unique IDs to thallium addresses
    std::map<int, std::string> addrs;
    if (get_addrs(hx->p->bootstrap.comm, *engine, addrs) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    // remove the loopback endpoint
    addrs.erase(hx->p->bootstrap.rank);

    EndpointGroup *eg = new EndpointGroup(rpc, hx->p->memory_pools.responses, hx->p->memory_pools.arrays, hx->p->memory_pools.buffers);
    if (!eg) {
        return TRANSPORT_ERROR;
    }

    // create mapping between unique IDs and ranks
    for(decltype(addrs)::value_type const &addr : addrs) {
        Endpoint_t server(new thallium::endpoint(engine->lookup(addr.second)));
        mlog(HXHIM_CLIENT_DBG, "Created Thallium endpoint %s", addr.second.c_str());

        // add the remote thallium endpoint to the tranport
        Endpoint* ep = new Endpoint(engine, rpc, server, hx->p->memory_pools.responses, hx->p->memory_pools.arrays, hx->p->memory_pools.buffers);
        hx->p->transport->AddEndpoint(addr.first, ep);
        mlog(HXHIM_CLIENT_DBG, "Created HXHIM endpoint from Thallium endpoint %s", addr.second.c_str());

        // if the rank was specified as part of the endpoint group, add the thallium endpoint to the endpoint group
        if (opts->p->endpointgroup.find(addr.first) != opts->p->endpointgroup.end()) {
            eg->AddID(addr.first, server);
            mlog(HXHIM_CLIENT_DBG, "Added Thallium endpoint %s to the endpoint group", addr.second.c_str());
        }
    }

    hx->p->transport->SetEndpointGroup(eg);
    hx->p->range_server_destroy = RangeServer::destroy;

    mlog(HXHIM_CLIENT_DBG, "Completed Thallium transport initialization");
    return TRANSPORT_SUCCESS;
}

#endif

/**
 * transport
 * Starts up the transport layer
 *
 * @param hx   the HXHIM instance
 * @param opts the HXHIM options
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::transport(hxhim_t *hx, hxhim_options_t *opts) {
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
            ret = init_transport_null(hx, opts);
            break;
        case Transport::TRANSPORT_MPI:
            ret = init_transport_mpi(hx, opts);
            break;
        #if HXHIM_HAVE_THALLIUM
        case Transport::TRANSPORT_THALLIUM:
            ret = init_transport_thallium(hx, opts);
            break;
        #endif
        default:
            mlog(HXHIM_CLIENT_ERR, "Received bad transport type to initialize %d", opts->p->transport->type);
            break;
    }

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
