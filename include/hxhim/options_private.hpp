#ifndef HXHIM_OPTIONS_PRIVATE_HPP
#define HXHIM_OPTIONS_PRIVATE_HPP

#include <set>
#include <string>

#include <mpi.h>

#include "hxhim/bootstrap.h"
#include "hxhim/constants.h"
#include "hxhim/hash.hpp"
#include "transport/options.hpp"
#include "utils/Histogram.hpp"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Structures for holding datastore configurations
 */
typedef struct hxhim_datastore_config {
    hxhim_datastore_config(const hxhim_datastore_t datastore)
        : type(datastore)
    {}

    const hxhim_datastore_t type;
} hxhim_datastore_config_t;

typedef struct hxhim_leveldb_config : hxhim_datastore_config_t {
    hxhim_leveldb_config()
        : hxhim_datastore_config_t(HXHIM_DATASTORE_LEVELDB)
    {}

    std::size_t id;
    std::string path;
    bool create_if_missing;
} hxhim_leveldb_config_t;

typedef struct hxhim_in_memory_config : hxhim_datastore_config_t {
    hxhim_in_memory_config()
        : hxhim_datastore_config_t(HXHIM_DATASTORE_IN_MEMORY)
    {}
} hxhim_in_memory_config_t;

/**
 * The entire collection of configuration options
 */
typedef struct hxhim_options_private {
    bootstrap_t mpi;                          // bootstrap information

    hxhim_datastore_config_t *datastore;      // configuration options for the selected datastore
    std::size_t datastore_count;

    std::string hash;
    Transport::Options *transport;

    std::set<int> endpointgroup;

    std::size_t queued_bputs;                 // number of batches to hold before sending PUTs asynchronously

    struct {
        std::size_t first_n;                  // number of datapoints used to generate histogram buckets
        Histogram::BucketGen::generator gen;  // string name of the histogram bucket generation method
        void *args;
    } histogram;
} hxhim_options_private_t;

#ifdef __cplusplus
}
#endif


#endif
