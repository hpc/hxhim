#ifndef HXHIM_OPTIONS_H
#define HXHIM_OPTIONS_H

#include <mpi.h>

#include "hxhim/constants.h"
#include "hxhim/hash.h"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct hxhim_datastore_config hxhim_datastore_config_t;

typedef struct hxhim_options_private hxhim_options_private_t;

typedef struct hxhim_options {
    hxhim_options_private_t *p;
} hxhim_options_t;

int hxhim_options_init(hxhim_options_t *opts);

/**
 * Functions for configuring HXHIM
 */
int hxhim_options_set_mpi_bootstrap(hxhim_options_t *opts, MPI_Comm comm);
int hxhim_options_set_debug_level(hxhim_options_t *opts, const int level);
int hxhim_options_set_datastores_per_range_server(hxhim_options_t *opts, const size_t count);
int hxhim_options_set_datastore_leveldb(hxhim_options_t *opts, const size_t id, const char *path, const int create_if_missing);
int hxhim_options_set_datastore_in_memory(hxhim_options_t *opts);
int hxhim_options_set_hash_name(hxhim_options_t *opts, const char *hash);
int hxhim_options_set_hash_function(hxhim_options_t *opts, hxhim_hash_t hash, void *args);
int hxhim_options_set_transport_mpi(hxhim_options_t *opts, const size_t listeners);
int hxhim_options_set_transport_thallium(hxhim_options_t *opts, const char *module);
int hxhim_options_add_endpoint_to_group(hxhim_options_t *opts, const int id);
int hxhim_options_clear_endpoint_group(hxhim_options_t *opts);
int hxhim_options_set_queued_bputs(hxhim_options_t *opts, const size_t count);
int hxhim_options_set_histogram_first_n(hxhim_options_t *opts, const size_t count);
int hxhim_options_set_histogram_bucket_gen_method(hxhim_options_t *opts, const char *method);

int hxhim_options_set_packed_alloc_size(hxhim_options_t *opts, const size_t alloc_size);
int hxhim_options_set_packed_regions(hxhim_options_t *opts, const size_t regions);
int hxhim_options_set_packed_name(hxhim_options_t *opts, const char *name);
int hxhim_options_set_buffers_alloc_size(hxhim_options_t *opts, const size_t alloc_size);
int hxhim_options_set_buffers_regions(hxhim_options_t *opts, const size_t regions);
int hxhim_options_set_buffers_name(hxhim_options_t *opts, const char *name);
int hxhim_options_set_bulks_alloc_size(hxhim_options_t *opts, const size_t alloc_size);
int hxhim_options_set_bulks_regions(hxhim_options_t *opts, const size_t regions);
int hxhim_options_set_bulks_name(hxhim_options_t *opts, const char *name);
int hxhim_options_set_keys_alloc_size(hxhim_options_t *opts, const size_t alloc_size);
int hxhim_options_set_keys_regions(hxhim_options_t *opts, const size_t regions);
int hxhim_options_set_keys_name(hxhim_options_t *opts, const char *name);
int hxhim_options_set_arrays_alloc_size(hxhim_options_t *opts, const size_t alloc_size);
int hxhim_options_set_arrays_regions(hxhim_options_t *opts, const size_t regions);
int hxhim_options_set_arrays_name(hxhim_options_t *opts, const char *name);
int hxhim_options_set_requests_alloc_size(hxhim_options_t *opts, const size_t alloc_size);
int hxhim_options_set_requests_regions(hxhim_options_t *opts, const size_t regions);
int hxhim_options_set_requests_name(hxhim_options_t *opts, const char *name);
int hxhim_options_set_responses_alloc_size(hxhim_options_t *opts, const size_t alloc_size);
int hxhim_options_set_responses_regions(hxhim_options_t *opts, const size_t regions);
int hxhim_options_set_responses_name(hxhim_options_t *opts, const char *name);
int hxhim_options_set_result_alloc_size(hxhim_options_t *opts, const size_t alloc_size);
int hxhim_options_set_result_regions(hxhim_options_t *opts, const size_t regions);
int hxhim_options_set_result_name(hxhim_options_t *opts, const char *name);
int hxhim_options_set_results_alloc_size(hxhim_options_t *opts, const size_t alloc_size);
int hxhim_options_set_results_regions(hxhim_options_t *opts, const size_t regions);
int hxhim_options_set_results_name(hxhim_options_t *opts, const char *name);

/** Default FixedBufferPool values */
int hxhim_options_set_bulks_default_alloc_size(hxhim_options_t *opts);


// Cleans up memory allocated inside opts, but not the opts variable itself
int hxhim_options_destroy(hxhim_options_t *opts);

/**
 * Functions that handle datastore configurations
 */
hxhim_datastore_config_t *hxhim_options_create_leveldb_config(const size_t id, const char *path, const int create_if_missing);
hxhim_datastore_config_t *hxhim_options_create_in_memory_config();

// Cleans up datastore config memory, including the config variable itself because the user will never be able to create their own config
void hxhim_options_datastore_config_destroy(hxhim_datastore_config_t *config);

#ifdef __cplusplus
}
#endif

#endif
