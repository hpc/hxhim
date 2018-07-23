#ifndef HXHIM_OPTIONS_PRIVATE_HPP
#define HXHIM_OPTIONS_PRIVATE_HPP

#include <string>

#include <mpi.h>

#include "bootstrap.h"
#include "constants.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct hxhim_backend_config {
    void *null;
} hxhim_backend_config_t;

typedef struct hxhim_mdhim_config : hxhim_backend_config_t {
    std::string path;
} hxhim_mdhim_config_t;

typedef struct hxhim_leveldb_config : hxhim_backend_config_t {
    std::string path;
    bool create_if_missing;
} hxhim_leveldb_config_t;

typedef struct hxhim_options_private {
    bootstrap_t mpi;                            // bootstrap information

    hxhim_backend_t backend;
    hxhim_backend_config_t *backend_config;     // depends on backend value

    hxhim_spo_type_t subject_type;
    hxhim_spo_type_t predicate_type;
    hxhim_spo_type_t object_type;

    std::size_t queued_bputs;                   // number of batches to hold before sending PUTs asynchronously

    std::size_t histogram_first_n;              // number of datapoints used to generate histogram buckets
    std::string histogram_bucket_gen_method;    // string name of the histogram bucket generation method
} hxhim_options_private_t;

#ifdef __cplusplus
}
#endif


#endif
