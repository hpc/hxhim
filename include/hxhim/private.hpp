#ifndef HXHIM_PRIVATE_HPP
#define HXHIM_PRIVATE_HPP

#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_map>

#include <mpi.h>

#include "datastore/datastore.hpp"
#include "hxhim/Results.hpp"
#include "hxhim/cache.hpp"
#include "hxhim/constants.h"
#include "hxhim/hash.hpp"
#include "hxhim/options.h"
#include "hxhim/struct.h"
#include "transport/transport.hpp"
#include "utils/enable_if_t.hpp"
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

    /** @decription Statistics for an instance of Transport */
    struct Stats {
        struct Op {
            /** @description Statistics on percentage of each packet filled */
            struct Filled {
                Filled(const int dst, const long double percent)
                    : dst(dst),
                      percent(percent)
                    {}

                Filled(const Filled &filled)
                    : dst(filled.dst),
                      percent(filled.percent)
                    {}

                Filled(const Filled &&filled)
                    : dst(std::move(filled.dst)),
                      percent(std::move(filled.percent))
                    {}

                Filled &operator=(const Filled &filled) {
                    dst = filled.dst;
                    percent = filled.percent;
                    return *this;
                }

                Filled &operator=(const Filled &&filled) {
                    dst = std::move(filled.dst);
                    percent = std::move(filled.percent);
                    return *this;
                }

                int dst;
                long double percent;
            };

            std::list<Filled> filled;
            mutable std::mutex mutex;
        };

        Op bput;
        Op bget;
        Op bgetop;
        Op bdel;
    };

    Stats stats;

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

/**
 * collect_fill_stats
 * Collects statistics on how much of a bulk packet was used before being sent into the transport
 *
 * @tparam msg  a single message
 * @param  op   the statistics structure for an operation (PUT, GET, GETOP, DEL)
 */
template <typename T>
void collect_fill_stats(T *msg, hxhim_private_t::Stats::Op &op) {
    if (msg) {
        // Collect packet filled percentage
        std::lock_guard<std::mutex> lock(op.mutex);
        op.filled.emplace_back(hxhim_private_t::Stats::Op::Filled(msg->dst, (double) msg->count / (double) msg->max_count));
    }
}

/**
 * collect_fill_stats
 * Collects statistics on how much of a bulk packet was used before being sent into the transport
 *
 * @param msgs the list (really map) of messages
 * @param op   the statistics structure for an operation (PUT, GET, GETOP, DEL)
 */
template <typename T>
void collect_fill_stats(const std::unordered_map<int, T *> &msgs, hxhim_private_t::Stats::Op &op) {
    // Collect packet filled percentage
    std::lock_guard<std::mutex> lock(op.mutex);
    for(REF(msgs)::value_type const & msg : msgs) {
        if (msg.second) {
            op.filled.emplace_back(hxhim_private_t::Stats::Op::Filled(msg.first, (double) msg.second->count / (double) msg.second->max_count));
        }
    }
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
