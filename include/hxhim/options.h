#ifndef HXHIM_OPTIONS_HPP
#define HXHIM_OPTIONS_HPP

#include <mpi.h>

#include "constants.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct hxhim_backend_config hxhim_backend_config_t;

typedef struct hxhim_options_private hxhim_options_private_t;

typedef struct hxhim_options {
    hxhim_options_private_t *p;
} hxhim_options_t;

int hxhim_options_init(hxhim_options_t *opts);

/**
 * Functions for configuring HXHIM
 */
int hxhim_options_set_mpi_bootstrap(hxhim_options_t *opts, MPI_Comm comm);
int hxhim_options_set_backend(hxhim_options_t *opts, const hxhim_backend_t backend, hxhim_backend_config_t *config);
int hxhim_options_set_queued_bputs(hxhim_options_t *opts, const size_t count);
int hxhim_options_set_histogram_first_n(hxhim_options_t *opts, const size_t count);
int hxhim_options_set_histogram_bucket_gen_method(hxhim_options_t *opts, const char *method);

// Cleans up memory allocated inside opts, but not the opts variable itself
int hxhim_options_destroy(hxhim_options_t *opts);

/**
 * Functions that handle backend configurations
 */
hxhim_backend_config_t *hxhim_options_create_mdhim_config(char *path);
hxhim_backend_config_t *hxhim_options_create_leveldb_config(char *path, const int create_if_missing);
hxhim_backend_config_t *hxhim_options_create_in_memory_config();

// Cleans up config memory, including the config variable itself because the user will never be able to create their own config
void hxhim_options_backend_config_destroy(hxhim_backend_config_t *config);

#ifdef __cplusplus
}
#endif

#endif
