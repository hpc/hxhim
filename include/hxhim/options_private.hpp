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
 * Structures for holding database configurations
 */
typedef struct hxhim_database_config {
    hxhim_database_config(const hxhim_database_t database)
        : type(database)
    {}

    const hxhim_database_t type;
} hxhim_database_config_t;

typedef struct hxhim_leveldb_config : hxhim_database_config_t {
    hxhim_leveldb_config()
        : hxhim_database_config_t(HXHIM_DATABASE_LEVELDB)
    {}

    std::size_t id;
    std::string path;
    bool create_if_missing;
} hxhim_leveldb_config_t;

typedef struct hxhim_in_memory_config : hxhim_database_config_t {
    hxhim_in_memory_config()
        : hxhim_database_config_t(HXHIM_DATABASE_IN_MEMORY)
    {}
} hxhim_in_memory_config_t;

/**
 * The entire collection of configuration options
 */
typedef struct hxhim_options_private {
    bootstrap_t mpi;                          // bootstrap information

    hxhim_database_config_t *database;        // configuration options for the selected database
    std::size_t database_count;

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
