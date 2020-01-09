#include <algorithm>
#include <cfloat>
#include <cmath>
#include <map>

#include "datastore/datastores.hpp"
#include "hxhim/Results_private.hpp"
#include "hxhim/config.hpp"
#include "hxhim/options_private.hpp"
#include "hxhim/private.hpp"
#include "hxhim/process.tpp"
#include "hxhim/range_server.hpp"
#include "hxhim/shuffle.hpp"
#include "transport/Messages/Messages.hpp"
#include "transport/transports.hpp"
#include "utils/macros.hpp"
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
      datastore(),
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
            hxhim::Results *res = hxhim::process<hxhim::PutData, Transport::Request::BPut, Transport::Response::BPut>(hx, head, hx->p->max_ops_per_send.puts);

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
    hx->p->max_ops_per_send.max     = opts->p->max_ops_per_send;
    hx->p->max_ops_per_send.puts    = opts->p->max_ops_per_send / HXHIM_PUT_MULTIPLIER;
    hx->p->max_ops_per_send.gets    = opts->p->max_ops_per_send;
    hx->p->max_ops_per_send.getops  = opts->p->max_ops_per_send;
    hx->p->max_ops_per_send.deletes = opts->p->max_ops_per_send;

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
    if (!valid(hx, opts)) {
        return HXHIM_ERROR;
    }

    hx->p->range_server.client_ratio = opts->p->client_ratio;
    hx->p->range_server.server_ratio = opts->p->server_ratio;

    // there must be more than 0 datastores
    if (!(hx->p->datastore.count = opts->p->datastore_count)) {
        return HXHIM_ERROR;
    }

    // allocate pointers
    hx->p->datastore.datastores = alloc_array<hxhim::datastore::Datastore *>(hx->p->datastore.count);

    const bool is_rs = hxhim::range_server::is_range_server(hx->p->bootstrap.rank, opts->p->client_ratio, opts->p->server_ratio);

    // create datastores
    for(std::size_t i = 0; i < hx->p->datastore.count; i++) {
        Histogram::Histogram *hist = new Histogram::Histogram(opts->p->histogram.first_n,
                                                              opts->p->histogram.gen,
                                                              opts->p->histogram.args);
        if ((opts->p->datastore->type == hxhim::datastore::IN_MEMORY) ||
            !is_rs) { // create unused datastores if this rank is not a range server
            hx->p->datastore.prefix = "";
            hx->p->datastore.datastores[i] = new hxhim::datastore::InMemory(hx,
                                                                            hxhim::datastore::get_id(hx, hx->p->bootstrap.rank, i),
                                                                            hist,
                                                                            hx->p->hash.name);
            mlog(HXHIM_CLIENT_INFO, "Initialized In-Memory in datastore[%zu]", i);
        }
        #if HXHIM_HAVE_LEVELDB
        else if (opts->p->datastore->type == hxhim::datastore::LEVELDB) {
            hxhim_leveldb_config_t *config = static_cast<hxhim_leveldb_config_t *>(opts->p->datastore);
            hx->p->datastore.prefix = config->prefix;
            hx->p->datastore.datastores[i] = new hxhim::datastore::leveldb(hx,
                                                                           hxhim::datastore::get_id(hx, hx->p->bootstrap.rank, i),
                                                                           hist,
                                                                           hx->p->hash.name, config->create_if_missing);
            mlog(HXHIM_CLIENT_INFO, "Initialized LevelDB in datastore[%zu]", i);
        }
        #endif
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

    if ((hx->p->datastore.count = opts->p->datastore_count) != 1) {
        return HXHIM_ERROR;
    }

    hx->p->datastore.datastores = alloc_array<hxhim::datastore::Datastore *>(hx->p->datastore.count);

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
    hx->p->async_put.max_queued = opts->p->start_async_put_at;

    // Set up queued PUT results list
    hx->p->async_put.results = construct<hxhim::Results>();

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

    if (hx->p->datastore.datastores) {
        for(std::size_t i = 0; i < hx->p->datastore.count; i++) {
            if (hx->p->datastore.datastores[i]) {
                hx->p->datastore.datastores[i]->Close();
                delete hx->p->datastore.datastores[i];
                hx->p->datastore.datastores[i] = nullptr;
            }
        }

        dealloc_array(hx->p->datastore.datastores, hx->p->datastore.count);
        hx->p->datastore.datastores = nullptr;
    }

    return HXHIM_SUCCESS;
}

/**
 * PutImpl
 * Add a PUT into the work queue
 * The mutex should be locked before this function is called.
 * hx and hx->p are not checked because they must have been
 * valid for this function to be called.
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
int hxhim::PutImpl(hxhim_t *hx,
                   void *subject, std::size_t subject_len,
                   void *predicate, std::size_t predicate_len,
                   enum hxhim_type_t object_type, void *object, std::size_t object_len) {
    mlog(HXHIM_CLIENT_INFO, "Foreground PUT Start (%s, %s, %s)", (char *) subject, (char *) predicate, (char *) object);
    hxhim::Unsent<hxhim::PutData> &puts = hx->p->queues.puts;

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
 * The mutex should be locked before this function is called.
 * hx and hx->p are not checked because they must have been
 * valid for this function to be called.
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
int hxhim::GetImpl(hxhim_t *hx,
                    void *subject, std::size_t subject_len,
                    void *predicate, std::size_t predicate_len,
                    enum hxhim_type_t object_type, void *object, std::size_t *object_len) {
    mlog(HXHIM_CLIENT_DBG, "GET Start");
    hxhim::GetData *get = construct<hxhim::GetData>();
    get->subject = construct<ReferenceBlob>(subject, subject_len);
    get->predicate = construct<ReferenceBlob>(predicate, predicate_len);
    get->object_type = object_type;
    get->object = object;
    get->object_len = object_len;

    mlog(HXHIM_CLIENT_DBG, "GET Insert into queue");
    hxhim::Unsent<hxhim::GetData> &gets = hx->p->queues.gets;
    gets.insert(get);
    gets.start_processing.notify_one();
    mlog(HXHIM_CLIENT_DBG, "GET Completed");
    return HXHIM_SUCCESS;
}

/**
 * DeleteImpl
 * Add a DELETE into the work queue
 * The mutex should be locked before this function is called.
 * hx and hx->p are not checked because they must have been
 * valid for this function to be called.
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to delete
 * @param subject_len  the length of the subject to delete
 * @param prediate     the prediate to delete
 * @param prediate_len the length of the prediate to delete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::DeleteImpl(hxhim_t *hx,
                      void *subject, std::size_t subject_len,
                      void *predicate, std::size_t predicate_len) {
    mlog(HXHIM_CLIENT_DBG, "DELETE Start");
    hxhim::DeleteData *del = construct<hxhim::DeleteData>();
    del->subject = construct<ReferenceBlob>(subject, subject_len);
    del->predicate = construct<ReferenceBlob>(predicate, predicate_len);

    mlog(HXHIM_CLIENT_DBG, "DELETE Insert into queue");
    hxhim::Unsent<hxhim::DeleteData> &dels = hx->p->queues.deletes;
    dels.insert(del);
    dels.start_processing.notify_one();

    mlog(HXHIM_CLIENT_DBG, "Delete Completed");
    return HXHIM_SUCCESS;
}
