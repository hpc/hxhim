#ifndef HXHIM_PRIVATE_HPP
#define HXHIM_PRIVATE_HPP

#include <atomic>
#include <mutex>
#include <thread>

#include <mpi.h>

#include "datastore/datastore.hpp"
#include "hxhim/Results.hpp"
#include "hxhim/cache.hpp"
#include "hxhim/constants.h"
#include "hxhim/hash.hpp"
#include "hxhim/options.h"
#include "hxhim/struct.h"
#include "transport/transport.hpp"
#include "utils/FixedBufferPool.hpp"

/**
 * hxhim_private
 * The actual HXHIM state
 *
 * hxhim_private is a struct because it the members are meant to
 * be accessed directly instead of through accessor/modifiers.
 *
 * Initialization is partially done through the constructor
 * and partially done through hxhim::init functions.
 * Destruction is done through hxhim::destroy functions.
 *
 * hxhim::init and hxhim::destroy should eventually be
 * moved into hxhim_private.
 */
typedef struct hxhim_private {
    hxhim_private();

    struct {
        MPI_Comm comm;
        int rank;
        int size;
    } bootstrap;

    std::atomic_bool running;                                  // whether or not HXHIM is running

    struct {
        std::size_t max;
        std::size_t puts;
        std::size_t gets;
        std::size_t getops;
        std::size_t deletes;
    } max_ops_per_send;

    // unsent data queues
    struct {
        hxhim::Unsent<hxhim::PutData> puts;
        hxhim::Unsent<hxhim::GetData> gets;
        hxhim::Unsent<hxhim::GetOpData> getops;
        hxhim::Unsent<hxhim::DeleteData> deletes;
    } queues;

    struct {
        std::string prefix;                                    // the datastore path name prefix
        hxhim::datastore::Datastore **datastores;              // fixed array of datastores mapped by rank and index: f(rank, index) -> datastore ID
        std::size_t count;                                     // number of datastores in this process
    } datastore;

    // asynchronous BPUT data
    struct {
        std::size_t max_queued;                                // number of batches to hold before sending PUTs asynchronously
        std::thread thread;                                    // the thread that pushes PUTs off the PUT queue asynchronously
        std::mutex mutex;                                      // mutex to the list of results from asynchronous PUT operations
        hxhim::Results *results;                               // the list of of PUT results
    } async_put;

    struct {
        std::string name;
        hxhim_hash_t func;                                     // the function used to determine which datastore should be used to perform an operation with
        void *args;                                            // extra arguments to pass into the hash function
    } hash;

    // Transport variables
    Transport::Transport *transport;

    // Range Server
    struct {
        std::size_t client_ratio;                              // client portion of client:server ratio
        std::size_t server_ratio;                              // server portion of client:server ratio
        void (*destroy)();                                     // Range server static variable cleanup
    } range_server;

    // Memory pools used to allocate space in HXHIM
    struct {
        FixedBufferPool *keys;                                 // at least (subject_len + sizeof(std::size_t) + predicate_len + sizeof(std::size_t))
        FixedBufferPool *buffers;                              // maximum size of smaller buffers (subjects, predicates, objects, etc)
        FixedBufferPool *ops_cache;                            // internal staging area of individual operations waiting to be packed and sent
        FixedBufferPool *arrays;                               // storage for bulk message internal arrays; size is max_bulk_ops.max * sizeof(void *)
        FixedBufferPool *requests;                             // should have enough space to allow for maximum number of requests queued before flushing
        FixedBufferPool *responses;                            // should have enough space to allow for responses from all queued requests
        FixedBufferPool *result;                               // should have enough space to allow for results from all returned responses
        FixedBufferPool *results;                              // should have enough space to allow for maximum number of valid results at once
    } memory_pools;

} hxhim_private_t;

namespace hxhim {

bool valid(hxhim_t *hx);
bool valid(hxhim_options_t *opts);
bool valid(hxhim_t *hx, hxhim_options_t *opts);

// HXHIM should (probably) be initialized in this order
namespace init {
int bootstrap    (hxhim_t *hx, hxhim_options_t *opts);
int running      (hxhim_t *hx, hxhim_options_t *opts);
int memory       (hxhim_t *hx, hxhim_options_t *opts);
int datastore    (hxhim_t *hx, hxhim_options_t *opts);
int one_datastore(hxhim_t *hx, hxhim_options_t *opts, const std::string &name);
int async_put    (hxhim_t *hx, hxhim_options_t *opts);
int hash         (hxhim_t *hx, hxhim_options_t *opts);
int transport    (hxhim_t *hx, hxhim_options_t *opts);
}

// HXHIM should (probably) be destroyed in this order
namespace destroy {
int bootstrap   (hxhim_t *hx);
int running     (hxhim_t *hx);
int memory      (hxhim_t *hx);
int transport   (hxhim_t *hx);
int hash        (hxhim_t *hx);
int async_put   (hxhim_t *hx);
int datastore   (hxhim_t *hx);
}

int PutImpl(hxhim_t *hx,
            void *subject, std::size_t subject_len,
            void *predicate, std::size_t predicate_len,
            enum hxhim_type_t object_type, void *object, std::size_t object_len);

int GetImpl(hxhim_t *hx,
            void *subject, std::size_t subject_len,
            void *predicate, std::size_t predicate_len,
            enum hxhim_type_t object_type);

int DeleteImpl(hxhim_t *hx,
               void *subject, std::size_t subject_len,
               void *predicate, std::size_t predicate_len);
}

#endif
