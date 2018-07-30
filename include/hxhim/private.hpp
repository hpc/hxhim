#ifndef HXHIM_PRIVATE_HPP
#define HXHIM_PRIVATE_HPP

#include <atomic>
#include <mutex>
#include <thread>

#include <mpi.h>

#include "hxhim/Results.hpp"
#include "hxhim/backend/base.hpp"
#include "hxhim/cache.hpp"
#include "hxhim/constants.h"
#include "hxhim/hash.hpp"
#include "hxhim/options.h"
#include "hxhim/struct.h"
#include "transport/transport.hpp"

typedef struct hxhim_private {
    hxhim_private();

    std::atomic_bool running;

    // unsent data queues
    hxhim::Unsent<hxhim::PutData> puts;
    hxhim::Unsent<hxhim::GetData> gets;
    hxhim::Unsent<hxhim::GetOpData> getops;
    hxhim::Unsent<hxhim::DeleteData> deletes;

    hxhim::backend::base **databases;
    std::size_t database_count;

    // asynchronous PUT data
    struct {
        std::size_t max_queued;                                // number of batches to hold before sending PUTs asynchronously
        std::thread thread;                                    // the thread that pushes PUTs off the PUT queue asynchronously
        std::mutex mutex;                                      // mutex to the list of results from asynchronous PUT operations
        hxhim::Results *results;                               // the list of of PUT results
    } async_put;

    // Transport variables
    hxhim::hash::Func hash;                                    // the function used to determine which database should be used to perform an operation with
    void *hash_args;
    Transport::Transport *transport;
    void (*range_server_destroy)();                            // Range server static variable cleanup
} hxhim_private_t;

namespace hxhim {

// HXHIM should (probably) be initialized in this order
namespace init {
int running     (hxhim_t *hx, hxhim_options_t *opts);
int range_server(hxhim_t *hx, hxhim_options_t *opts);
int database     (hxhim_t *hx, hxhim_options_t *opts);
int one_database (hxhim_t *hx, hxhim_options_t *opts, const std::string &name);
int async_put   (hxhim_t *hx, hxhim_options_t *opts);
int hash        (hxhim_t *hx, hxhim_options_t *opts);
int transport   (hxhim_t *hx, hxhim_options_t *opts);
}

// HXHIM should (probably) be destroyed in this order
namespace destroy {
int running     (hxhim_t *hx);
int transport   (hxhim_t *hx);
int hash        (hxhim_t *hx);
int async_put   (hxhim_t *hx);
int database     (hxhim_t *hx);
int range_server(hxhim_t *hx);
}

}

#endif
