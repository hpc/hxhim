#ifndef HXHIM_OPTIONS_PRIVATE_HPP
#define HXHIM_OPTIONS_PRIVATE_HPP

#include <set>
#include <string>

#include <mpi.h>

#include "datastore/constants.h"
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

#if HXHIM_HAVE_LEVELDB
typedef struct hxhim_leveldb_config : hxhim_datastore_config_t {
    hxhim_leveldb_config()
        : hxhim_datastore_config_t(HXHIM_DATASTORE_LEVELDB)
    {}

    std::size_t id;
    std::string prefix;
    bool create_if_missing;
} hxhim_leveldb_config_t;
#endif

typedef struct hxhim_in_memory_config : hxhim_datastore_config_t {
    hxhim_in_memory_config()
        : hxhim_datastore_config_t(HXHIM_DATASTORE_IN_MEMORY)
    {}
} hxhim_in_memory_config_t;

typedef struct hxhim_fbp_config {
    hxhim_fbp_config(const std::size_t alloc_size = 0, const std::size_t regions = 0, const std::string &name = "")
        : alloc_size(alloc_size),
          regions(regions),
          name(name)
    {}

    std::size_t alloc_size;
    std::size_t regions;
    std::string name;
} hxhim_fbp_config_t;

/**
 * The entire collection of configuration options
 */
typedef struct hxhim_options_private {
    MPI_Comm comm;                            // bootstrap communicator

    int debug_level;

    hxhim_datastore_config_t *datastore;      // configuration options for the selected datastore
    std::size_t datastore_count;

    struct {
        std::string name;
        hxhim_hash_t func;
        void *args;
    } hash;

    Transport::Options *transport;

    std::set<int> endpointgroup;

    std::size_t queued_bputs;                 // number of batches to hold before sending PUTs asynchronously

    struct {
        std::size_t first_n;                  // number of datapoints used to generate histogram buckets
        HistogramBucketGenerator_t gen;       // string name of the histogram bucket generation method
        void *args;
    } histogram;

    // settings for FixedBufferPool
    hxhim_fbp_config_t packed;
    hxhim_fbp_config_t buffers;
    hxhim_fbp_config_t bulks;
    hxhim_fbp_config_t keys;
    hxhim_fbp_config_t arrays;
    hxhim_fbp_config_t requests;
    hxhim_fbp_config_t responses;
    hxhim_fbp_config_t result;
    hxhim_fbp_config_t results;

} hxhim_options_private_t;

#ifdef __cplusplus
}
#endif


#endif
