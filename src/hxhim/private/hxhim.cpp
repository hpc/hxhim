#include <algorithm>
#include <cfloat>
#include <cmath>
#include <iomanip>
#include <map>

#include "datastore/datastores.hpp"
#include "hxhim/config.hpp"
#include "hxhim/private/Results.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/private/options.hpp"
#include "hxhim/private/process.hpp"
#include "transport/transports.hpp"
#include "utils/memory.hpp"
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
void clean(hxhim_t *, Data *node) {
    while (node) {
        Data *next = node->next;
        destruct(node);
        node = next;
    }
}

hxhim_private::hxhim_private()
    : bootstrap(),
      running(false),
      max_ops_per_send(),
      queues(),
      datastores(),
      async_put(),
      hash(),
      transport(nullptr),
      range_server()
{}

/**
 * backgroundPUT
 * The thread that runs when the number of full batches crosses the queued bputs threshold
 *
 * @param hx      the HXHIM context
 */
static void backgroundPUT(hxhim_t *hx) {
    if (!hx || !hx->p) {
        return;
    }

    mlog(HXHIM_CLIENT_DBG, "Started background PUT thread");

    while (hx->p->running) {
        hxhim::PutData *head = nullptr;    // the first PUT to process
        bool force = false;                // whether or not FlushPuts was called

        // hold unsent.mutex just long enough to move queued PUTs to send queue
        {
            hxhim::Unsent<hxhim::PutData> &unsent = hx->p->queues.puts;

            // Wait until any of the following is true
            //    1. HXHIM is no longer running
            //    2. The number of queued PUTs passes the threshold
            //    3. The PUTs are being forced to flush
            std::unique_lock<std::mutex> lock(unsent.mutex);
            while (hx->p->running && (unsent.count < hx->p->async_put.max_queued) && !unsent.force) {
                mlog(HXHIM_CLIENT_DBG, "Waiting for %zu PUTs (currently have %zu)", hx->p->async_put.max_queued, unsent.count);
                unsent.start_processing.wait(lock, [&]() -> bool { return !hx->p->running || (unsent.count >= hx->p->async_put.max_queued) || unsent.force; });
            }

            mlog(HXHIM_CLIENT_DBG, "Moving %zu queued PUTs into process queue", unsent.count);

            // move all PUTs into this thread for processing
            head = unsent.head;

            unsent.head = nullptr;
            unsent.tail = nullptr;
            unsent.count = 0;

            // record whether or not this loop was forced, since the lock is not held
            force = unsent.force;
            unsent.force = false;
        }

        // process the queued PUTs
        mlog(HXHIM_CLIENT_DBG, "Processing queued PUTs");
        {
            // process the batch and save the results
            hxhim::Results *res = hxhim::process<hxhim::PutData, Transport::Request::BPut, Transport::Response::BPut>(hx, head, hx->p->max_ops_per_send[Transport::Message::Type::BPUT]);

            {
                std::unique_lock<std::mutex> lock(hx->p->async_put.mutex);
                hx->p->async_put.results->Append(res);
            }

            destruct(res);
        }
        mlog(HXHIM_CLIENT_DBG, "Done processing queued PUTs");

        // if this flush was forced, notify FlushPuts
        if (force) {
            hxhim::Unsent<hxhim::PutData> &unsent = hx->p->queues.puts;
            std::unique_lock<std::mutex> lock(unsent.mutex);
            unsent.done_processing.notify_all();
        }

        // // clean up in case previous loop stopped early
        // clean(hx, head);
    }

    mlog(HXHIM_CLIENT_DBG, "Background PUT thread stopping");
}

/**
 * valid
 * Checks if hx is valid
 *
 * @param hx   the HXHIM instance
 * @param true if ready, else false
 */
bool hxhim::valid(hxhim_t *hx) {
    return hx && hx->p;
}

/**
 * valid
 * Checks if opts are valid
 *
 * @param opts the HXHIM options
 * @param true if ready, else false
 */
bool hxhim::valid(hxhim_options_t *opts) {
    return opts && opts->p;
}

/**
 * valid
 * Checks if hx and opts are ready to be used
 *
 * @param hx   the HXHIM instance
 * @param opts the HXHIM options
 * @param true if ready, else false
 */
bool hxhim::valid(hxhim_t *hx, hxhim_options_t *opts) {
    return valid(hx) && valid(opts);
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
    if (opts->p->max_ops_per_send < HXHIM_PUT_MULTIPLIER) {
        mlog(HXHIM_CLIENT_ERR, "There should be at least %d operations per send", HXHIM_PUT_MULTIPLIER);
        return HXHIM_SUCCESS;
    }

    // size of each set of queued messages
    hx->p->max_ops_per_send[Transport::Message::Type::BPUT]    = opts->p->max_ops_per_send / HXHIM_PUT_MULTIPLIER;
    hx->p->max_ops_per_send[Transport::Message::Type::BGET]    = opts->p->max_ops_per_send;
    hx->p->max_ops_per_send[Transport::Message::Type::BGETOP]  = opts->p->max_ops_per_send;
    hx->p->max_ops_per_send[Transport::Message::Type::BDELETE] = opts->p->max_ops_per_send;

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
            Histogram::Histogram *hist = new Histogram::Histogram(opts->p->histogram.first_n,
                                                                  opts->p->histogram.gen,
                                                                  opts->p->histogram.args);

            if (opts->p->datastore->type == hxhim::datastore::IN_MEMORY) {
                hx->p->datastores[i] = new hxhim::datastore::InMemory(hx->p->bootstrap.rank,
                                                                      hxhim::datastore::get_id(hx, hx->p->bootstrap.rank, i),
                                                                      hist,
                                                                      hx->p->hash.name);
                mlog(HXHIM_CLIENT_INFO, "Initialized In-Memory in datastore[%zu]", i);
            }
            #if HXHIM_HAVE_LEVELDB
            else if (opts->p->datastore->type == hxhim::datastore::LEVELDB) {
                hxhim::datastore::leveldb::Config *config = static_cast<hxhim::datastore::leveldb::Config *>(opts->p->datastore);
                hx->p->datastores[i] = new hxhim::datastore::leveldb(hx->p->bootstrap.rank,
                                                                     hxhim::datastore::get_id(hx, hx->p->bootstrap.rank, i),
                                                                     hist,
                                                                     config->prefix,
                                                                     hx->p->hash.name,
                                                                     config->create_if_missing);
                mlog(HXHIM_CLIENT_INFO, "Initialized LevelDB in datastore[%zu]", i);
            }
            #endif
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

    Histogram::Histogram *hist = new Histogram::Histogram(opts->p->histogram.first_n,
                                                          opts->p->histogram.gen,
                                                          opts->p->histogram.args);

    // ignore configuration hash - everything goes into here
    hx->p->hash.name = "local";
    hx->p->hash.func = hxhim_hash_RankZero;
    hx->p->hash.args = nullptr;

    // Start the datastore
    switch (opts->p->datastore->type) {
        case hxhim::datastore::IN_MEMORY:
            hx->p->datastores[0] = new hxhim::datastore::InMemory(hx->p->bootstrap.rank,
                                                                  0,
                                                                  hist,
                                                                  name);
            mlog(HXHIM_CLIENT_INFO, "Initialized single In-Memory datastore");
            break;
        #if HXHIM_HAVE_LEVELDB
        case hxhim::datastore::LEVELDB:
            hx->p->datastores[0] = new hxhim::datastore::leveldb(hx->p->bootstrap.rank,
                                                                 hist,
                                                                 name,
                                                                 false);
            mlog(HXHIM_CLIENT_INFO, "Initialized single LevelDB datastore");
            break;
        #endif
        default:
            break;
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
    if (!valid(hx, opts)) {
        return HXHIM_ERROR;
    }

    if (init::running(hx, opts) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    // Set number of bulk puts to queue up before sending
    hx->p->async_put.max_queued = opts->p->start_async_put_at;

    // Set up queued PUT results list
    hx->p->async_put.results = nullptr;

    // // Start the background thread
    // hx->p->async_put.thread = std::thread(backgroundPUT, hx);

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

    if (!opts->p->client_ratio) {
        mlog(HXHIM_CLIENT_ERR, "Client ratio must be at least 1");
        return HXHIM_ERROR;
    }

    if (!opts->p->server_ratio) {
        mlog(HXHIM_SERVER_ERR, "Server ratio must be at least 1");
        return HXHIM_ERROR;
    }

    delete hx->p->transport;
    if (!(hx->p->transport = new Transport::Transport())) {
        return HXHIM_ERROR;
    }

    int ret = TRANSPORT_ERROR;
    switch (opts->p->transport->type) {
        case Transport::TRANSPORT_NULL:
            ret = TRANSPORT_SUCCESS;
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

    if (hx->p->range_server.destroy) {
        hx->p->range_server.destroy();
    }

    delete hx->p->transport;
    hx->p->transport = nullptr;

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

    // if (hx->p->async_put.thread.joinable()) {
    //     hx->p->async_put.thread.join();
    // }

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
        destruct(hx->p->async_put.results);
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

    for(int i = 0; i < hx->p->bootstrap.size; i++) {
        MPI_Barrier(hx->p->bootstrap.comm);
        if (hx->p->bootstrap.rank == i) {
            for(hxhim::datastore::Datastore *&ds : hx->p->datastores) {
                if (ds) {
                    ds->Close();
                    delete ds;
                    ds = nullptr;
                }
            }
        }
    }

    hx->p->datastores.resize(0);

    return HXHIM_SUCCESS;
}

std::ostream &hxhim::print_stats(hxhim_t *hx,
                                 std::ostream &stream,
                                 const std::string &indent) {
    return hx->p->stats.print(hx->p->max_ops_per_send, stream, indent);
}

/**
 * PutImpl
 * Add a PUT into the work queue
 * hx and hx->p are not checked because they must have been
 * valid for this function to be called.
 *
 * @param puts           the queue to place the PUT in
 * @param subject        the subject to put
 * @param subject_len    the length of the subject to put
 * @param predicate      the prediate to put
 * @param predicate_len  the length of the prediate to put
 * @param object_type    the type of the object
 * @param object         the object to put
 * @param object_len     the length of the object
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::PutImpl(hxhim::Unsent<hxhim::PutData> &puts,
                   void *subject, std::size_t subject_len,
                   void *predicate, std::size_t predicate_len,
                   enum hxhim_type_t object_type, void *object, std::size_t object_len) {
    mlog(HXHIM_CLIENT_INFO, "Foreground PUT Start (%p, %p, %p)", subject, predicate, object);

    hxhim::PutData *put = construct<hxhim::PutData>();
    put->subject = construct<ReferenceBlob>(subject, subject_len);
    put->predicate = construct<ReferenceBlob>(predicate, predicate_len);
    put->object_type = object_type;
    put->object = construct<ReferenceBlob>(object, object_len);

    mlog(HXHIM_CLIENT_DBG, "Foreground PUT Insert SPO into queue");
    puts.insert(put);

    puts.start_processing.notify_one();

    mlog(HXHIM_CLIENT_DBG, "Foreground PUT Completed");
    return HXHIM_SUCCESS;
}

/**
 * GetImpl
 * Add a GET into the work queue
 * hx and hx->p are not checked because they must have been
 * valid for this function to be called.
 *
 * @param gets           the queue to place the GET in
 * @param subject        the subject to put
 * @param subject_len    the length of the subject to put
 * @param predicate      the prediate to put
 * @param predicate_len  the length of the prediate to put
 * @param object_type    the type of the object
 * @param object         the prediate to put
 * @param object_len     the length of the prediate to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::GetImpl(hxhim::Unsent<hxhim::GetData> &gets,
                   void *subject, std::size_t subject_len,
                   void *predicate, std::size_t predicate_len,
                   enum hxhim_type_t object_type) {
    mlog(HXHIM_CLIENT_DBG, "GET Start");

    hxhim::GetData *get = construct<hxhim::GetData>();
    get->subject = construct<ReferenceBlob>(subject, subject_len);
    get->predicate = construct<ReferenceBlob>(predicate, predicate_len);
    get->object_type = object_type;

    mlog(HXHIM_CLIENT_DBG, "GET Insert into queue");
    gets.insert(get);
    gets.start_processing.notify_one();

    mlog(HXHIM_CLIENT_DBG, "GET Completed");
    return HXHIM_SUCCESS;
}

/**
 * GetOpImpl
 * Add a GETOP into the work queue
 * hx and hx->p are not checked because they must have been
 * valid for this function to be called.
 *
 * @param getops         the queue to place the GETOP in
 * @param subject        the subject to put
 * @param subject_len    the length of the subject to put
 * @param predicate      the prediate to put
 * @param predicate_len  the length of the prediate to put
 * @param object_type    the type of the object
 * @param num_records    the number of records to get
 * @param op             the operation to run
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::GetOpImpl(hxhim::Unsent<hxhim::GetOpData> &getops,
                     void *subject, std::size_t subject_len,
                     void *predicate, std::size_t predicate_len,
                     enum hxhim_type_t object_type,
                     std::size_t num_records, enum hxhim_get_op_t op) {
    mlog(HXHIM_CLIENT_DBG, "GETOP Start");

    hxhim::GetOpData *getop = construct<hxhim::GetOpData>();
    getop->subject = construct<ReferenceBlob>(subject, subject_len);
    getop->predicate = construct<ReferenceBlob>(predicate, predicate_len);
    getop->object_type = object_type;
    getop->num_recs = num_records;
    getop->op = op;

    mlog(HXHIM_CLIENT_DBG, "GETOP Insert into queue");
    getops.insert(getop);
    getops.start_processing.notify_one();

    mlog(HXHIM_CLIENT_DBG, "GETOP Completed");
    return HXHIM_SUCCESS;
}

/**
 * DeleteImpl
 * Add a DELETE into the work queue
 * hx and hx->p are not checked because they must have been
 * valid for this function to be called.
 *
 * @param dels         the queue to place the DELETE in
 * @param subject      the subject to delete
 * @param subject_len  the length of the subject to delete
 * @param prediate     the prediate to delete
 * @param prediate_len the length of the prediate to delete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::DeleteImpl(hxhim::Unsent<hxhim::DeleteData> &dels,
                      void *subject, std::size_t subject_len,
                      void *predicate, std::size_t predicate_len) {
    mlog(HXHIM_CLIENT_DBG, "DELETE Start");

    hxhim::DeleteData *del = construct<hxhim::DeleteData>();
    del->subject = construct<ReferenceBlob>(subject, subject_len);
    del->predicate = construct<ReferenceBlob>(predicate, predicate_len);

    mlog(HXHIM_CLIENT_DBG, "DELETE Insert into queue");
    dels.insert(del);
    dels.start_processing.notify_one();

    mlog(HXHIM_CLIENT_DBG, "Delete Completed");
    return HXHIM_SUCCESS;
}