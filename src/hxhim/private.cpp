#include <cfloat>

#include "datastore/datastores.hpp"
#include "hxhim/config.hpp"
#include "hxhim/local_client.hpp"
#include "hxhim/options_private.hpp"
#include "hxhim/private.hpp"
#include "hxhim/shuffle.hpp"
#include "hxhim/triplestore.hpp"
#include "transport/backend/backends.hpp"
#include "transport/transport.hpp"
#include "utils/MemoryManagers.hpp"

hxhim_private::hxhim_private()
    : running(false),
      puts(),
      gets(),
      getops(),
      deletes(),
      datastores(nullptr),
      datastore_count(0),
      async_put(),
      transport(nullptr),
      range_server_destroy(nullptr)
{
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

    const std::size_t total = HXHIM_PUT_MULTIPLER * count;                              // total number of triples that will be PUT

    Transport::Request::BPut local(HXHIM_MAX_BULK_GET_OPS);
    local.src = hx->mpi.rank;
    local.dst = hx->mpi.rank;
    local.count = 0;

    Transport::Request::BPut **remote = new Transport::Request::BPut *[hx->mpi.size](); // list of destination servers (not datastores) and messages to those destinations
    for(std::size_t i = 0; i < hx->mpi.size; i++) {
        remote[i] = new Transport::Request::BPut(HXHIM_MAX_BULK_GET_OPS);
        remote[i]->src = hx->mpi.rank;
        remote[i]->dst = i;
        remote[i]->count = 0;
    }

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
                            &local, remote);

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

    hxhim::Results *res = new hxhim::Results();

    // PUT the batch
    if (hx->mpi.size > 1) {
        Transport::Response::BPut *responses = hx->p->transport->BPut(hx->mpi.size, remote);
        for(Transport::Response::BPut *curr = responses; curr; curr = curr->next) {
            for(std::size_t i = 0; i < curr->count; i++) {
                res->Add(new hxhim::Results::Put(curr, i));
            }
        }
        delete responses;
    }

    if (local.count) {
        Transport::Response::BPut *responses = local_client_bput(hx, &local);
        for(Transport::Response::BPut *curr = responses; curr; curr = curr->next) {
            for(std::size_t i = 0; i < curr->count; i++) {
                res->Add(new hxhim::Results::Put(curr, i));
            }
        }
        delete responses;
    }

    // cleanup
    for(std::size_t i = 0; i < hx->mpi.size; i++) {
        delete remote[i];
    }
    delete [] remote;

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

    while (hx->p->running) {
        hxhim::PutData *head = nullptr;    // the first batch of PUTs to process

        bool force = false;                // whether or not FlushPuts was called
        hxhim::PutData *last = nullptr;    // pointer to the last batch of PUTs; only valid if force is true
        std::size_t last_count = 0;        // number of SPO triples in the last batch

        // hold unsent.mutex just long enough to move queued PUTs to send queue
        {
            hxhim::Unsent<hxhim::PutData> &unsent = hx->p->puts;
            std::unique_lock<std::mutex> lock(unsent.mutex);
            while (hx->p->running && (unsent.full_batches < hx->p->async_put.max_queued) && !unsent.force) {
                unsent.start_processing.wait(lock, [&]() -> bool { return !hx->p->running || (unsent.full_batches >= hx->p->async_put.max_queued) || unsent.force; });
            }

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
        while (hx->p->running && head) {
            // process the batch and save the results
            hxhim::Results *res = put_core(hx, head, HXHIM_MAX_BULK_PUT_OPS);

            // go to the next batch
            hxhim::PutData *next = head->next;
            delete head;
            head = next;

            {
                std::unique_lock<std::mutex> lock(hx->p->async_put.mutex);
                hx->p->async_put.results->Append(res);
            }

            delete res;
        }

        // if this flush was forced, notify FlushPuts
        if (force) {
            if (hx->p->running) {
                // process the batch
                hxhim::Results *res = put_core(hx, last, last_count);

                delete last;
                last = nullptr;

                {
                    std::unique_lock<std::mutex> lock(hx->p->async_put.mutex);
                    hx->p->async_put.results->Append(res);
                }

                delete res;
            }

            hxhim::Unsent<hxhim::PutData> &unsent = hx->p->puts;
            std::unique_lock<std::mutex> lock(unsent.mutex);
            unsent.done_processing.notify_all();
        }

        // clean up in case previous loop stopped early
        hxhim::clean(head);
        delete last;
    }
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

    if (!(hx->p->datastore_count = opts->p->datastore_count)) {
        return HXHIM_ERROR;
    }

    if (!(hx->p->datastores = new hxhim::datastore::Datastore *[hx->p->datastore_count])) {
        return HXHIM_ERROR;
    }

    for(std::size_t i = 0; i < hx->p->datastore_count; i++) {
        // Start the datastore
        switch (opts->p->datastore->type) {
            case HXHIM_DATASTORE_LEVELDB:
                {
                    hxhim_leveldb_config_t *config = static_cast<hxhim_leveldb_config_t *>(opts->p->datastore);
                    hx->p->datastores[i] = new hxhim::datastore::leveldb(hx,
                                                                        config->id,
                                                                        opts->p->histogram.first_n,
                                                                        opts->p->histogram.gen,
                                                                        opts->p->histogram.args,
                                                                        config->path, config->create_if_missing);
                }
                break;
            case HXHIM_DATASTORE_IN_MEMORY:
                {
                    hx->p->datastores[i] = new hxhim::datastore::InMemory(hx,
                                                                         i,
                                                                         opts->p->histogram.first_n,
                                                                         opts->p->histogram.gen,
                                                                         opts->p->histogram.args);
                }
                break;
            default:
                break;
        }

        if (!hx->p->datastores[i]) {
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

    if ((hx->p->datastore_count = opts->p->datastore_count) != 1) {
        return HXHIM_ERROR;
    }

    if (!(hx->p->datastores = new hxhim::datastore::Datastore *[hx->p->datastore_count])) {
        return HXHIM_ERROR;
    }

    // Start the datastore
    switch (opts->p->datastore->type) {
        case HXHIM_DATASTORE_LEVELDB:
            hx->p->datastores[0] = new hxhim::datastore::leveldb(hx,
                                                                opts->p->histogram.first_n,
                                                                opts->p->histogram.gen,
                                                                opts->p->histogram.args,
                                                                name);
            break;
        case HXHIM_DATASTORE_IN_MEMORY:
            hx->p->datastores[0] = new hxhim::datastore::InMemory(hx,
                                                                 0,
                                                                 opts->p->histogram.first_n,
                                                                 opts->p->histogram.gen,
                                                                 opts->p->histogram.args);
            break;
        default:
            break;
    }

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
    if (!valid(hx, opts)) {
        return HXHIM_ERROR;
    }

    if (init::running(hx, opts) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    // Set up queued PUT results list
    if (!(hx->p->async_put.results = new hxhim::Results())) {
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

    hx->p->hash = opts->p->hash;
    hx->p->hash_args = opts->p->hash_args;

    return HXHIM_SUCCESS;
}

/**
 * init_transport_mpi
 *
 * @param hx             the HXHIM instance
 * @param opts           the HXHIM options
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
static int init_transport_mpi(hxhim_t *hx, hxhim_options_t *opts) {
    if (!valid(hx, opts) || !hx->p->transport) {
        return HXHIM_ERROR;
    }

    using namespace Transport::MPI;

    // Do not allow MPI_COMM_NULL
    if (opts->p->mpi.comm == MPI_COMM_NULL) {
        return TRANSPORT_ERROR;
    }

    // Get the memory pool used for storing messages
    Options *config = static_cast<Options *>(opts->p->transport);
    FixedBufferPool *fbp = Memory::Pool(config->alloc_size, config->regions);
    if (!fbp) {
        return TRANSPORT_ERROR;
    }

    // give the range server access to the memory buffer
    RangeServer::init(hx, fbp, config->listeners);

    EndpointGroup *eg = new EndpointGroup(opts->p->mpi.comm, fbp);
    if (!eg) {
        return TRANSPORT_ERROR;
    }

    // create mapping between unique IDs and ranks
    for(int i = 0; i < opts->p->mpi.size; i++) {
        // MPI ranks map 1:1 with the boostrap MPI rank
        hx->p->transport->AddEndpoint(i, new Endpoint(opts->p->mpi.comm, i, fbp, hx->p->running));

        // if the rank was specified as part of the endpoint group, add the rank to the endpoint group
        if (opts->p->endpointgroup.find(i) != opts->p->endpointgroup.end()) {
            eg->AddID(i, i);
        }
    }

    // remove loopback endpoint
    hx->p->transport->RemoveEndpoint(hx->mpi.rank);

    hx->p->transport->SetEndpointGroup(eg);
    hx->p->range_server_destroy = RangeServer::destroy;

    return TRANSPORT_SUCCESS;
}

/**
 * init_transport_thallium
 *
 * @param hx   the HXHIM instance
 * @param opts the HXHIM options
 * @param TRANSPORT_SUCCESS or TRANSPORT_ERROR
 */
static int init_transport_thallium(hxhim_t *hx, hxhim_options_t *opts) {
    if (!valid(hx, opts)) {
        return HXHIM_ERROR;
    }

    using namespace Transport::Thallium;

    // create the engine (only 1 instance per process)
    Options *config = static_cast<Options *>(opts->p->transport);
    Engine_t engine(new thallium::engine(config->module, THALLIUM_SERVER_MODE, true, -1),
                                         [](thallium::engine *engine) {
                                             engine->finalize();
                                             delete engine;
                    });

    // create client to range server RPC
    RPC_t rpc(new thallium::remote_procedure(engine->define(RangeServer::CLIENT_TO_RANGE_SERVER_NAME,
                                                            RangeServer::process)));

    // give the range server access to the mdhim_t data
    RangeServer::init(hx);

    // wait for every engine to start up
    MPI_Barrier(opts->p->mpi.comm);

    // get a mapping of unique IDs to thallium addresses
    std::map<int, std::string> addrs;
    if (get_addrs(opts->p->mpi.comm, engine, addrs) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    // remove the loopback endpoint
    addrs.erase(opts->p->mpi.rank);

    EndpointGroup *eg = new EndpointGroup(rpc);
    if (!eg) {
        return TRANSPORT_ERROR;
    }

    // create mapping between unique IDs and ranks
    for(std::pair<const int, std::string> const &addr : addrs) {
        Endpoint_t server(new thallium::endpoint(engine->lookup(addr.second)));

        // add the remote thallium endpoint to the tranport
        Endpoint* ep = new Endpoint(engine, rpc, server);
        hx->p->transport->AddEndpoint(addr.first, ep);

        // if the rank was specified as part of the endpoint group, add the thallium endpoint to the endpoint group
        if (opts->p->endpointgroup.find(addr.first) != opts->p->endpointgroup.end()) {
            eg->AddID(addr.first, server);
        }
    }

    hx->p->transport->SetEndpointGroup(eg);
    hx->p->range_server_destroy = RangeServer::destroy;

    return TRANSPORT_SUCCESS;
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
    if (!valid(hx, opts)) {
        return HXHIM_ERROR;
    }

    delete hx->p->transport;
    hx->p->transport = nullptr;
    if (!(hx->p->transport = new Transport::Transport())) {
        return HXHIM_ERROR;
    }

    int ret = TRANSPORT_ERROR;
    switch (opts->p->transport->type) {
        case Transport::TRANSPORT_MPI:
            ret = init_transport_mpi(hx, opts);
            break;
        case Transport::TRANSPORT_THALLIUM:
            ret = init_transport_thallium(hx, opts);
            break;
        default:
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

    hx->p->hash = nullptr;
    hx->p->hash_args = nullptr;

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
    hx->p->puts.start_processing.notify_all();
    hx->p->puts.done_processing.notify_all();

    if (hx->p->async_put.thread.joinable()) {
        hx->p->async_put.thread.join();
    }

    // clear out unflushed work in the work queue
    hxhim::clean(hx->p->puts.head);
    hx->p->puts.head = nullptr;
    hxhim::clean(hx->p->gets.head);
    hx->p->gets.head = nullptr;
    hxhim::clean(hx->p->getops.head);
    hx->p->getops.head = nullptr;
    hxhim::clean(hx->p->deletes.head);
    hx->p->deletes.head = nullptr;

    {
        std::unique_lock<std::mutex>(hx->p->async_put.mutex);
        delete hx->p->async_put.results;
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

    if (hx->p->datastores) {
        for(std::size_t i = 0; i < hx->p->datastore_count; i++) {
            if (hx->p->datastores[i]) {
                hx->p->datastores[i]->Close();
                delete hx->p->datastores[i];
                hx->p->datastores[i] = nullptr;
            }
        }

        delete [] hx->p->datastores;
        hx->p->datastores = nullptr;
    }

    return HXHIM_SUCCESS;
}
