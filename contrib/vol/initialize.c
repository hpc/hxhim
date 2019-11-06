#include "initialize.h"

#include <mpi.h>

#include "hxhim/hxhim.h"

static bool fill_options(hxhim_options_t *opts) {
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    return ((hxhim_options_init(opts)                                             == HXHIM_SUCCESS) &&
            (hxhim_options_set_mpi_bootstrap(opts, MPI_COMM_WORLD)                == HXHIM_SUCCESS) &&
            (hxhim_options_set_debug_level(opts, MLOG_ERR)                        == HXHIM_SUCCESS) &&
            (hxhim_options_set_client_ratio(opts, 1)                              == HXHIM_SUCCESS) &&
            (hxhim_options_set_server_ratio(opts, 1)                              == HXHIM_SUCCESS) &&
            (hxhim_options_set_datastores_per_range_server(opts, 1)               == HXHIM_SUCCESS) &&
            (hxhim_options_set_datastore_leveldb(opts, rank, "/tmp/hxhim", 1)     == HXHIM_SUCCESS) &&
            (hxhim_options_set_transport_thallium(opts, "na+sm")                  == HXHIM_SUCCESS) &&
            (hxhim_options_set_hash_name(opts, "MY_RANK")                         == HXHIM_SUCCESS) &&
            (hxhim_options_set_keys_alloc_size(opts, 32)                          == HXHIM_SUCCESS) &&
            (hxhim_options_set_keys_regions(opts, 1)                              == HXHIM_SUCCESS) &&
            (hxhim_options_set_buffers_alloc_size(opts, 1)                        == HXHIM_SUCCESS) &&
            (hxhim_options_set_buffers_regions(opts, 1)                           == HXHIM_SUCCESS) &&
            (hxhim_options_set_ops_cache_regions(opts, 1)                         == HXHIM_SUCCESS) &&
            (hxhim_options_set_arrays_alloc_size(opts, 32 * sizeof(void *))       == HXHIM_SUCCESS) &&
            (hxhim_options_set_arrays_regions(opts, 128)                          == HXHIM_SUCCESS) &&
            (hxhim_options_set_requests_regions(opts, 2)                          == HXHIM_SUCCESS) &&
            (hxhim_options_set_client_packed_alloc_size(opts, 128)                == HXHIM_SUCCESS) &&
            (hxhim_options_set_client_packed_regions(opts, 2)                     == HXHIM_SUCCESS) &&
            (hxhim_options_set_rs_packed_alloc_size(opts, 128)                    == HXHIM_SUCCESS) &&
            (hxhim_options_set_rs_packed_regions(opts, 2)                         == HXHIM_SUCCESS) &&
            (hxhim_options_set_responses_regions(opts, 2)                         == HXHIM_SUCCESS) &&
            (hxhim_options_set_result_regions(opts, 16)                           == HXHIM_SUCCESS) &&
            (hxhim_options_set_results_regions(opts, 4)                           == HXHIM_SUCCESS) &&
            (hxhim_options_set_start_async_put_at(opts, 0)                        == HXHIM_SUCCESS) &&
            (hxhim_options_set_maximum_ops_per_send(opts, 1)                      == HXHIM_SUCCESS) &&
            (hxhim_options_set_histogram_first_n(opts, 10)                        == HXHIM_SUCCESS) &&
            (hxhim_options_set_histogram_bucket_gen_method(opts, "10_BUCKETS")    == HXHIM_SUCCESS) &&
            true);
}

herr_t H5VL_hxhim_initialize(hid_t vipl_id) {
    MPI_Init(NULL, NULL);

    /* MPI_Comm_size(MPI_COMM_WORLD, &size); */
    /* MPI_Comm_rank(MPI_COMM_WORLD, &rank); */

    /* fill_options(&opts); */
    /* hxhimOpen(&hx, &opts); */

    MPI_Barrier(MPI_COMM_WORLD);

    fprintf(stderr, "%d %s\n", __LINE__, __func__);

    return 0;
}
