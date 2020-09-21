#ifndef HXHIM_OPTIONS_PRIVATE_HPP
#define HXHIM_OPTIONS_PRIVATE_HPP

#include <set>
#include <string>

#include <mpi.h>

#include "datastore/datastores.hpp"
#include "hxhim/constants.h"
#include "hxhim/hash.h"
#include "hxhim/options.hpp"
#include "transport/Options.hpp"
#include "utils/Histogram.hpp"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * The entire collection of configuration options
 */
typedef struct hxhim_options_private {
    MPI_Comm comm;                         // bootstrap communicator

    int debug_level;

    // client:server
    std::size_t client_ratio;
    std::size_t server_ratio;

    hxhim::datastore::Config *datastore;   // configuration options for the selected datastore
    std::size_t datastore_count;

    std::size_t max_ops_per_send;          // the maximum number of operations that can be sent in one packet
    std::size_t start_async_put_at;        // number of PUTs to hold before sending PUTs asynchronously

    struct {
        std::string name;
        hxhim_hash_t func;
        void *args;
    } hash;

    Transport::Options *transport;

    std::set<int> endpointgroup;

    struct Histogram::Config histogram;
} hxhim_options_private_t;

#ifdef __cplusplus
}
#endif

// Private functions for setting options that have side effects
int hxhim_options_set_requests_regions_in_config(hxhim_options_t *opts, const size_t regions);

/**
 * Functions that handle datastore configurations
 */
hxhim::datastore::Config *hxhim_options_create_leveldb_config(const size_t id, const char *prefix, const int create_if_missing);
hxhim::datastore::Config *hxhim_options_create_in_memory_config();

/* Cleans up datastore config memory, including the config variable itself because the user will never be able to create their own config */
void hxhim_options_datastore_config_destroy(hxhim::datastore::Config *config);

#endif
