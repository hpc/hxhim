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
#include "message/Messages.hpp"
#include "transport/transport.hpp"
#include "utils/type_traits.hpp"

namespace hxhim {

// list of packets of a given type
template <typename Request_t,
          typename = enable_if_t<is_child_of<Message::Request::Request, Request_t>::value> >
using QueueTarget = std::list<Request_t *>;

// index is range server id, not rank
template <typename Request_t,
          typename = enable_if_t<is_child_of<Message::Request::Request, Request_t>::value> >
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
        struct {
            std::size_t ops;               // max slots to allocate
            std::size_t size;              // max bytes to allow
        } max_per_request;

        struct {
            hxhim::Queues<Message::Request::BPut> queue;
            std::mutex mutex;
            std::condition_variable start_processing; // check whether or not enough PUTs have been queued
            bool flushed;                             // true if flush was called
            bool processing;                          // true if data was popped off of the queue and has not been placed on the results queue yet
            std::size_t count;
        } puts;
        hxhim::Queues<Message::Request::BGet>       gets;
        hxhim::Queues<Message::Request::BGetOp>     getops;
        hxhim::Queues<Message::Request::BDelete>    deletes;
        hxhim::Queues<Message::Request::BHistogram> histograms;

        std::vector<int> ds_to_rank;
    } queues;

    // asynchronous BPUT data
    // handles processing of requests and results
    struct {
        bool enabled;
        std::size_t max_queued;            // number of batches to hold before sending PUTs asynchronously
        std::thread thread;                // the thread that pushes PUTs off the PUT queue asynchronously
        std::mutex mutex;
        std::condition_variable done;
        bool done_check;                   // whether or not the background puts have completed
        hxhim::Results *results;           // the list of of PUT results
    } async_puts;

    struct {
        std::string name;
        hxhim_hash_t func;                 // the function used to determine which datastore should be used to perform an operation with
        void *args;                        // extra arguments to pass into the hash function
    } hash;

    // Transport variables
    Transport::Transport *transport;

    struct {
        Datastore::HistNames_t names;      // all ranks have the list of histogram names
        struct Histogram::Config config;   // common settings for all histograms
        bool read;                         // whether or not to read existing histograms on open
        bool write;                        // whether or not to write histograms on close
    } histograms;

    // Range Server
    struct {
        int id;                            // > 0 if this is a range server; else -1
        std::size_t client_ratio;          // client portion of client:server ratio
        std::size_t server_ratio;          // server portion of client:server ratio
        std::size_t total_range_servers;   // total number of range servers across all ranks

        struct {
            std::string prefix;            // prefix path of datastore
            std::string basename;          // prefix of each datastore name
            std::string postfix;           // string to append to each datastore name
            std::size_t per_server;        // number of datastores per range server
            std::size_t total;             // total number of datastores across all ranks

            // f(datastore ID) = (rank, offset)
            std::vector<Datastore::Datastore *> ds;
        } datastores;
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
int async_puts   (hxhim_t *hx, hxhim_options_t *opts);
}

namespace destroy {
int bootstrap   (hxhim_t *hx);
int running     (hxhim_t *hx);
int async_puts  (hxhim_t *hx);
int transport   (hxhim_t *hx);
int datastore   (hxhim_t *hx);
int queues      (hxhim_t *hx);
int hash        (hxhim_t *hx);
}

void wait_for_background_puts(hxhim_t *hx, const bool drop_queued = false);
void serial_puts(hxhim_t *hx);

// this will probably be moved to the public side
std::ostream &print_stats(hxhim_t *hx,
                          std::ostream &stream,
                          const std::string &indent = "    ");

int PutImpl(hxhim_t *hx,
            Queues<Message::Request::BPut> &puts,
            Blob subject,
            Blob predicate,
            Blob object,
            const hxhim_put_permutation_t permutations);

int GetImpl(hxhim_t *hx,
            Queues<Message::Request::BGet> &gets,
            Blob subject,
            Blob predicate,
            enum hxhim_data_t object_type);

int GetOpImpl(hxhim_t *hx,
              Queues<Message::Request::BGetOp> &getops,
              Blob subject,
              Blob predicate,
              enum hxhim_data_t object_type,
              std::size_t num_records, enum hxhim_getop_t op);

int DeleteImpl(hxhim_t *hx,
               Queues<Message::Request::BDelete> &dels,
               Blob subject,
               Blob predicate);

int HistogramImpl(hxhim_t *hx,
                  Queues<Message::Request::BHistogram> &hists,
                  const int rs_id,
                  const char *name, const std::size_t name_len);
}

#endif
