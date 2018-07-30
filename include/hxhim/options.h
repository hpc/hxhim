#ifndef HXHIM_OPTIONS_HPP
#define HXHIM_OPTIONS_HPP

#include <mpi.h>

#include "constants.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct hxhim_database_config hxhim_database_config_t;

typedef struct hxhim_options_private hxhim_options_private_t;

typedef struct hxhim_options {
    hxhim_options_private_t *p;
} hxhim_options_t;

int hxhim_options_init(hxhim_options_t *opts);

/**
 * Functions for configuring HXHIM
 */
int hxhim_options_set_mpi_bootstrap(hxhim_options_t *opts, MPI_Comm comm);
int hxhim_options_set_databases_per_range_server(hxhim_options_t *opts, const size_t count);
int hxhim_options_set_database_leveldb(hxhim_options_t *opts, const size_t id, const char *path, const int create_if_missing);
int hxhim_options_set_database_in_memory(hxhim_options_t *opts);
int hxhim_options_set_hash(hxhim_options_t *opts, const char *hash);
int hxhim_options_set_transport_mpi(hxhim_options_t *opts, const size_t memory_alloc_size, const size_t memory_regions, const size_t listeners);
int hxhim_options_set_transport_thallium(hxhim_options_t *opts, const char *module);
int hxhim_options_add_endpoint_to_group(hxhim_options_t *opts, const int id);
int hxhim_options_clear_endpoint_group(hxhim_options_t *opts);
int hxhim_options_set_queued_bputs(hxhim_options_t *opts, const size_t count);
int hxhim_options_set_histogram_first_n(hxhim_options_t *opts, const size_t count);
int hxhim_options_set_histogram_bucket_gen_method(hxhim_options_t *opts, const char *method);

// Cleans up memory allocated inside opts, but not the opts variable itself
int hxhim_options_destroy(hxhim_options_t *opts);

/**
 * Functions that handle database configurations
 */
hxhim_database_config_t *hxhim_options_create_leveldb_config(const size_t id, const char *path, const int create_if_missing);
hxhim_database_config_t *hxhim_options_create_in_memory_config();

// Cleans up database config memory, including the config variable itself because the user will never be able to create their own config
void hxhim_options_database_config_destroy(hxhim_database_config_t *config);

#ifdef __cplusplus
}
#endif

#endif
