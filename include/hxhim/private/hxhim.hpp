#ifndef HXHIM_PRIVATE_HPP
#define HXHIM_PRIVATE_HPP

#include <atomic>
#include <condition_variable>
#include <list>
#include <mutex>
#include <ostream>
#include <sstream>
#include <thread>
#include <vector>

#include <mpi.h>

#include "datastore/datastore.hpp"
#include "hxhim/Results.hpp"
#include "hxhim/constants.h"
#include "hxhim/hash.h"
#include "hxhim/options.h"
#include "hxhim/private/Stats.hpp"
#include "hxhim/struct.h"
#include "transport/Messages/Messages.hpp"
#include "transport/transport.hpp"
#include "utils/type_traits.hpp"

namespace hxhim {

// list of packets of a given type
template <typename Request_t,
          typename = enable_if_t<is_child_of<Transport::Request::Request, Request_t>::value> >
using QueueTarget = std::list<Request_t *>;

// index is range server id, not rank
template <typename Request_t,
          typename = enable_if_t<is_child_of<Transport::Request::Request, Request_t>::value> >
using Queues = std::vector<QueueTarget<Request_t> >;

}

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
    ~hxhim_private();

    Stats::Chronopoint epoch;

    struct {
        MPI_Comm comm;
        int rank;
        int size;
    } bootstrap;

    std::atomic_bool running;              // whether or not HXHIM is running

    // unsent data queues
    struct {
        std::size_t max_ops_per_send;

        struct {
            hxhim::Queues<Transport::Request::BPut> queue;
            #if ASYNC_PUTS
            std::mutex mutex;
            std::condition_variable start_processing;
            bool flushed;
            #endif
            std::size_t count;
        } puts;
        hxhim::Queues<Transport::Request::BGet>       gets;
        hxhim::Queues<Transport::Request::BGetOp>     getops;
        hxhim::Queues<Transport::Request::BDelete>    deletes;
        hxhim::Queues<Transport::Request::BHistogram> histograms;

        std::vector<int> rs_to_rank;
    } queues;

    // asynchronous BPUT data
    struct {
        std::size_t max_queued;            // number of batches to hold before sending PUTs asynchronously
        #if ASYNC_PUTS
        std::thread thread;                // the thread that pushes PUTs off the PUT queue asynchronously
        std::mutex mutex;
        std::condition_variable done;
        bool done_check;
        #endif
        hxhim::Results *results;           // the list of of PUT results
    } async_put;

    // local datastore (max 1 per server)
    // f(rank) = datastore ID
    datastore::Datastore *datastore;

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
        std::size_t total_range_servers;   // total number of range servers across all ranks
    } range_server;

    hxhim::Stats::Global stats;
    std::stringstream print_buffer;

} hxhim_private_t;

namespace hxhim {

bool valid(hxhim_t *hx);
bool valid(hxhim_t *hx, hxhim_options_t *opts);

namespace init {
int bootstrap    (hxhim_t *hx, hxhim_options_t *opts);
int running      (hxhim_t *hx, hxhim_options_t *opts);
int hash         (hxhim_t *hx, hxhim_options_t *opts);
int datastore    (hxhim_t *hx, hxhim_options_t *opts);
int one_datastore(hxhim_t *hx, hxhim_options_t *opts, const std::string &name);
int transport    (hxhim_t *hx, hxhim_options_t *opts);
int queues       (hxhim_t *hx, hxhim_options_t *opts);
int async_put    (hxhim_t *hx, hxhim_options_t *opts);
}

namespace destroy {
int bootstrap   (hxhim_t *hx);
int running     (hxhim_t *hx);
int async_put   (hxhim_t *hx);
int transport   (hxhim_t *hx);
int datastore   (hxhim_t *hx);
int queues      (hxhim_t *hx);
int hash        (hxhim_t *hx);
}

#if ASYNC_PUTS
void wait_for_background_puts(hxhim_t *hx, const bool clear_queued = false);
#else
void serial_puts(hxhim_t *hx);
#endif

// this will probably be moved to the public side
std::ostream &print_stats(hxhim_t *hx,
                          std::ostream &stream,
                          const std::string &indent = "    ");

int PutImpl(hxhim_t *hx,
            Queues<Transport::Request::BPut> &puts,
            Blob subject,
            Blob predicate,
            Blob object);

int GetImpl(hxhim_t *hx,
            Queues<Transport::Request::BGet> &gets,
            Blob subject,
            Blob predicate,
            enum hxhim_data_t object_type);

int GetOpImpl(hxhim_t *hx,
              Queues<Transport::Request::BGetOp> &getops,
              Blob subject,
              Blob predicate,
              enum hxhim_data_t object_type,
              std::size_t num_records, enum hxhim_getop_t op);

int DeleteImpl(hxhim_t *hx,
               Queues<Transport::Request::BDelete> &dels,
               Blob subject,
               Blob predicate);

int HistogramImpl(hxhim_t *hx,
                  Queues<Transport::Request::BHistogram> &hists,
                  const int rs_id);
}

#endif
