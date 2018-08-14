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

typedef struct hxhim_private {
    hxhim_private();

    struct {
        MPI_Comm comm;
        int rank;
        int size;
    } bootstrap;

    std::atomic_bool running;                                  // whether or not HXHIM is running

    // unsent data queues
    hxhim::Unsent<hxhim::PutData> puts;
    hxhim::Unsent<hxhim::GetData> gets;
    hxhim::Unsent<hxhim::GetOpData> getops;
    hxhim::Unsent<hxhim::DeleteData> deletes;

    struct {
        hxhim::datastore::Datastore **datastores;              // fixed array of datastores mapped by rank and index: f(rank, index) -> datastore ID
        std::size_t count;                                     // number of datastores in this process
    } datastore;

    // asynchronous PUT data
    struct {
        std::size_t max_queued;                                // number of batches to hold before sending PUTs asynchronously
        std::thread thread;                                    // the thread that pushes PUTs off the PUT queue asynchronously
        std::mutex mutex;                                      // mutex to the list of results from asynchronous PUT operations
        hxhim::Results *results;                               // the list of of PUT results
    } async_put;

    struct {
        hxhim_hash_t func;                                     // the function used to determine which datastore should be used to perform an operation with
        void *args;                                            // extra arguments to pass into the hash function
    } hash;

    // Transport variables
    Transport::Transport *transport;
    void (*range_server_destroy)();                            // Range server static variable cleanup
} hxhim_private_t;

namespace hxhim {

// HXHIM should (probably) be initialized in this order
namespace init {
int bootstrap    (hxhim_t *hx, hxhim_options_t *opts);
int running      (hxhim_t *hx, hxhim_options_t *opts);
int range_server (hxhim_t *hx, hxhim_options_t *opts);
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
int transport   (hxhim_t *hx);
int hash        (hxhim_t *hx);
int async_put   (hxhim_t *hx);
int datastore   (hxhim_t *hx);
int range_server(hxhim_t *hx);
}

}

#endif
