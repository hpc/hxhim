#ifndef HXHIM_OPTIONS_H
#define HXHIM_OPTIONS_H

#include <mpi.h>

#include "hxhim/constants.h"
#include "hxhim/hash.h"
#include "datastore/transform.h"
#include "utils/Histogram.h"

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
int hxhim_options_set_client_ratio(hxhim_options_t *opts, const size_t ratio);
int hxhim_options_set_server_ratio(hxhim_options_t *opts, const size_t ratio);
int hxhim_options_set_open_init_datastore(hxhim_options_t *opts, const int init);
int hxhim_options_datastore_histograms(hxhim_options_t *opts, const int read, const int write);
int hxhim_options_set_datastore_in_memory(hxhim_options_t *opts);
#if HXHIM_HAVE_LEVELDB
int hxhim_options_set_datastore_leveldb(hxhim_options_t *opts, const char *prefix, const int create_if_missing);
#endif
#if HXHIM_HAVE_ROCKSDB
int hxhim_options_set_datastore_rocksdb(hxhim_options_t *opts, const char *prefix, const int create_if_missing);
#endif
int hxhim_options_set_hash_name(hxhim_options_t *opts, const char *name);
int hxhim_options_set_hash_function(hxhim_options_t *opts, const char *name, hxhim_hash_t func, void *args);
int hxhim_options_set_transform_numeric_values(hxhim_options_t *opts, const char neg, const char pos, const size_t float_precision, const size_t double_precision);
int hxhim_options_set_transform_function(hxhim_options_t *opts, const enum hxhim_data_t type,
                                         hxhim_encode_func encode, void *encode_extra,
                                         hxhim_decode_func decode, void *decode_extra);
int hxhim_options_set_transport_null(hxhim_options_t *opts);
int hxhim_options_set_transport_mpi(hxhim_options_t *opts, const size_t listeners);
#if HXHIM_HAVE_THALLIUM
int hxhim_options_set_transport_thallium(hxhim_options_t *opts, const char *module);
#endif
int hxhim_options_add_endpoint_to_group(hxhim_options_t *opts, const int id);
int hxhim_options_clear_endpoint_group(hxhim_options_t *opts);

/** Asynchronous PUT Settings */
int hxhim_options_set_maximum_queued_ops(hxhim_options_t *opts, const size_t count);
int hxhim_options_set_start_async_put_at(hxhim_options_t *opts, const size_t count);

/* maximum size of any buffer being sent to a single destination */
int hxhim_options_set_maximum_ops_per_send(hxhim_options_t *opts, const size_t count);

int hxhim_options_set_histogram_first_n(hxhim_options_t *opts, const size_t count);
int hxhim_options_set_histogram_bucket_gen_name(hxhim_options_t *opts, const char *method);
int hxhim_options_set_histogram_bucket_gen_function(hxhim_options_t *opts, HistogramBucketGenerator_t gen, void *args);
int hxhim_options_add_histogram_track_predicate(hxhim_options_t *opts, const char *name, const size_t name_len);

/* Cleans up memory allocated inside opts, but not the opts variable itself */
int hxhim_options_destroy(hxhim_options_t *opts);

#ifdef __cplusplus
}
#endif

#endif
