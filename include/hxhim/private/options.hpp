#ifndef HXHIM_OPTIONS_PRIVATE_HPP
#define HXHIM_OPTIONS_PRIVATE_HPP

#include <set>
#include <string>

#include <mpi.h>

#include "datastore/datastores.hpp"
#include "datastore/transform.hpp"
#include "hxhim/constants.h"
#include "hxhim/hash.h"
#include "hxhim/options.hpp"
#include "transport/Options.hpp"
#include "utils/Histogram.hpp"

extern "C"
{

/**
 * The entire collection of configuration options
 */
typedef struct hxhim_options_private {
    MPI_Comm comm;                             // bootstrap communicator

    int debug_level;

    // client:server
    std::size_t client_ratio;
    std::size_t server_ratio;

    struct {
        std::size_t ops;                       // the maximum number of operations that can be requested in one packet
        std::size_t size;                      // the maximum nuber of bytes to send per request
    } max_per_request;

    struct {
        bool enabled;
        std::size_t start_at;                  // number of PUTs to hold before sending PUTs asynchronously
    } async_puts;

    struct {
        std::string name;
        hxhim_hash_t func;
        void *args;
    } hash;

    Transport::Options *transport;
    std::set<int> endpointgroup;

    // whether or not the datastore objects should open the underlying datastore.
    // not set in configuration file, but can be modified in code
    bool open_init_datastore = true;
    Datastore::Config *datastore;              // configuration options for the selected datastore

    Datastore::Transform::Callbacks transform; // callbacks for transforming user data into datastore-suitable data
    struct {
        Datastore::HistNames_t names;          // names of histograms that should keep track of PUTs
        struct Histogram::Config config;       // common settings for all histograms

        bool read;                             // if open_init_datastore == true, whether or not to try to find and read histograms
        bool write;
    } histograms;
} hxhim_options_private_t;

}

// Private functions for setting options that have side effects
int hxhim_options_set_requests_regions_in_config(hxhim_options_t *opts, const size_t regions);

/* Cleans up datastore config memory, including the config variable itself because the user will never be able to create their own config */
void hxhim_options_datastore_config_destroy(Datastore::Config *config);

namespace hxhim {
bool valid(hxhim_options_t *opts);
}

#endif
