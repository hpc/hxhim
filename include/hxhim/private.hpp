#ifndef HXHIM_PRIVATE_HPP
#define HXHIM_PRIVATE_HPP

#include <atomic>
#include <mutex>
#include <ostream>
#include <thread>
#include <vector>

#include <mpi.h>

#include "datastore/datastore.hpp"
#include "hxhim/Results.hpp"
#include "hxhim/Stats.hpp"
#include "hxhim/cache.hpp"
#include "hxhim/constants.h"
#include "hxhim/hash.hpp"
#include "hxhim/options.h"
#include "hxhim/struct.h"
#include "transport/transport.hpp"
#include "utils/type_traits.hpp"
#include "utils/macros.hpp"

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

    std::atomic_bool running;              // whether or not HXHIM is running

    std::map<Transport::Message::Type, std::size_t> max_ops_per_send;

    // unsent data queues
    struct {
        hxhim::Unsent<hxhim::PutData> puts;
        hxhim::Unsent<hxhim::GetData> gets;
        hxhim::Unsent<hxhim::GetOpData> getops;
        hxhim::Unsent<hxhim::DeleteData> deletes;
    } queues;

    // local datastores
    // f(rank, index) = datastore ID
    std::vector<hxhim::datastore::Datastore *> datastores;
    std::size_t total_datastores;          // total number of datastores across all ranks

    // asynchronous BPUT data
    struct {
        std::size_t max_queued;            // number of batches to hold before sending PUTs asynchronously
        std::thread thread;                // the thread that pushes PUTs off the PUT queue asynchronously
        std::mutex mutex;                  // mutex to the list of results from asynchronous PUT operations
        hxhim::Results *results;           // the list of of PUT results
    } async_put;

    struct {
        std::string name;
        hxhim_hash_t func;                 // the function used to determine which datastore should be used to perform an operation with
        void *args;                        // extra arguments to pass into the hash function
    } hash;

    // Transport variables
    Transport::Transport *transport;

    // Range Server
    struct {
        std::size_t client_ratio;          // client portion of client:server ratio
        std::size_t server_ratio;          // server portion of client:server ratio
        void (*destroy)();                 // Range server static variable cleanup
    } range_server;

    hxhim::Stats::Stats stats;

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

// this will probably be moved to the public side
std::ostream &print_stats(hxhim_t *hx,
                          std::ostream &stream,
                          const std::string &indent = "    ");

int PutImpl(hxhim::Unsent<hxhim::PutData> &puts,
            void *subject, std::size_t subject_len,
            void *predicate, std::size_t predicate_len,
            enum hxhim_type_t object_type, void *object, std::size_t object_len);

int GetImpl(hxhim::Unsent<hxhim::GetData> &gets,
            void *subject, std::size_t subject_len,
            void *predicate, std::size_t predicate_len,
            enum hxhim_type_t object_type);

int GetOpImpl(hxhim::Unsent<hxhim::GetOpData> &getops,
              void *subject, std::size_t subject_len,
              void *predicate, std::size_t predicate_len,
              enum hxhim_type_t object_type,
              std::size_t num_records, enum hxhim_get_op_t op);

int DeleteImpl(hxhim::Unsent<hxhim::DeleteData> &dels,
               void *subject, std::size_t subject_len,
               void *predicate, std::size_t predicate_len);
}

#endif
