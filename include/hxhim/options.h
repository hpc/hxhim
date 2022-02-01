#ifndef HXHIM_OPTIONS_H
#define HXHIM_OPTIONS_H

#include <mpi.h>

#include "datastore/transform.h"
#include "hxhim/constants.h"
#include "hxhim/hash.h"
#include "hxhim/struct.h"
#include "utils/Histogram.h"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Functions for configuring HXHIM
 */
int hxhim_set_mpi_bootstrap(hxhim_t *hx, MPI_Comm comm);
int hxhim_set_debug_level(hxhim_t *hx, const int level);
int hxhim_set_client_ratio(hxhim_t *hx, const size_t ratio);
int hxhim_set_server_ratio(hxhim_t *hx, const size_t ratio);
int hxhim_set_datastores_per_server(hxhim_t *hx, const size_t datastores_per_server);
int hxhim_set_open_init_datastore(hxhim_t *hx, const int init);
int hxhim_datastore_histograms(hxhim_t *hx, const int read, const int write);
int hxhim_set_datastore_name(hxhim_t *hx, const char *prefix, const char *basename, const char *postfix);
int hxhim_set_datastore_in_memory(hxhim_t *hx);
#if HXHIM_HAVE_LEVELDB
int hxhim_set_datastore_leveldb(hxhim_t *hx,
                                        const int create_if_missing);
#endif
#if HXHIM_HAVE_ROCKSDB
int hxhim_set_datastore_rocksdb(hxhim_t *hx,
                                        const int create_if_missing);
#endif
int hxhim_set_hash_name(hxhim_t *hx, const char *name);
int hxhim_set_hash_function(hxhim_t *hx, const char *name, hxhim_hash_t func, void *args);
int hxhim_set_transform_numeric_values(hxhim_t *hx, const char neg, const char pos, const size_t float_precision, const size_t double_precision);
int hxhim_set_transform_function(hxhim_t *hx, const enum hxhim_data_t type,
                                         hxhim_encode_func encode, void *encode_extra,
                                         hxhim_decode_func decode, void *decode_extra);
int hxhim_set_transport_null(hxhim_t *hx);
int hxhim_set_transport_mpi(hxhim_t *hx, const size_t listeners);
#if HXHIM_HAVE_THALLIUM
int hxhim_set_transport_thallium(hxhim_t *hx, const char *module, const int thread_count);
#endif
int hxhim_add_endpoint_to_group(hxhim_t *hx, const int id);
int hxhim_clear_endpoint_group(hxhim_t *hx);

/** Asynchronous PUT Settings */
int hxhim_set_start_async_puts_at(hxhim_t *hx, const size_t count);

/* maximum size of any buffer being sent to a single destination */
int hxhim_set_maximum_ops_per_request(hxhim_t *hx, const size_t count);
int hxhim_set_maximum_size_per_request(hxhim_t *hx, const size_t size);

int hxhim_set_histogram_first_n(hxhim_t *hx, const size_t count);
int hxhim_set_histogram_bucket_gen_name(hxhim_t *hx, const char *method);
int hxhim_set_histogram_bucket_gen_function(hxhim_t *hx, HistogramBucketGenerator_t gen, void *args);
int hxhim_add_histogram_track_predicate(hxhim_t *hx, const char *name, const size_t name_len);

#ifdef __cplusplus
}
#endif

#endif
