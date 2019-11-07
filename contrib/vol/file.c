#include "file.h"

#include <stdlib.h>

static bool fill_options(hxhim_options_t *opts, const char * name) {
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    return ((hxhim_options_init(opts)                                             == HXHIM_SUCCESS) &&
            (hxhim_options_set_mpi_bootstrap(opts, MPI_COMM_WORLD)                == HXHIM_SUCCESS) &&
            (hxhim_options_set_debug_level(opts, MLOG_ERR)                        == HXHIM_SUCCESS) &&
            (hxhim_options_set_client_ratio(opts, 1)                              == HXHIM_SUCCESS) &&
            (hxhim_options_set_server_ratio(opts, 1)                              == HXHIM_SUCCESS) &&
            (hxhim_options_set_datastores_per_range_server(opts, 1)               == HXHIM_SUCCESS) &&
            (hxhim_options_set_datastore_leveldb(opts, rank, name, 1)             == HXHIM_SUCCESS) &&
            (hxhim_options_set_transport_thallium(opts, "na+sm")                  == HXHIM_SUCCESS) &&
            (hxhim_options_set_hash_name(opts, "MY_RANK")                         == HXHIM_SUCCESS) &&
            (hxhim_options_set_keys_alloc_size(opts, 32)                          == HXHIM_SUCCESS) &&
            (hxhim_options_set_keys_regions(opts, 1)                              == HXHIM_SUCCESS) &&
            (hxhim_options_set_buffers_alloc_size(opts, 16)                       == HXHIM_SUCCESS) &&
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

/* H5F routines */
void *H5VL_hxhim_file_create(const char *name, unsigned flags, hid_t fcpl_id,
                             hid_t fapl_id, hid_t dxpl_id, void **req){
    /* struct info_t info; */
    /* if (H5Pget_vol_info(fapl_id, (void **)&info) < 0) { */
    /*     fprintf(stderr, "could not get info\n"); */
    /*     return NULL; */
    /* } */

    struct file_info_t * info = malloc(sizeof(struct file_info_t));
    if (!fill_options(&info->opts, name)                     ||
        (hxhimOpen(&info->hx, &info->opts) != HXHIM_SUCCESS)) {
        fprintf(stderr, "%d %s error\n", __LINE__, __func__);
        return NULL;
    }

    fprintf(stderr, "%d %s %p\n", __LINE__, __func__, name);
    return info;
}

void *H5VL_hxhim_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req){
    /* struct file_info_t info; */
    /* if (H5Pget_vol_info(fapl_id, (void **)&info) < 0) { */
    /*     fprintf(stderr, "could not get info\n"); */
    /*     return NULL; */
    /* } */

    struct file_info_t * info = malloc(sizeof(struct file_info_t));
    if ((fill_options(&info->opts, name)   != HXHIM_SUCCESS) ||
        (hxhimOpen(&info->hx, &info->opts) != HXHIM_SUCCESS)) {
        fprintf(stderr, "%d %s error\n", __LINE__, __func__);
        return NULL;
    }

    fprintf(stderr, "%d %s %p\n", __LINE__, __func__, name);
    return info;
}

herr_t H5VL_hxhim_file_close(void *file, hid_t dxpl_id, void **req){
    struct file_info_t * info = file;
    hxhimClose(&info->hx);
    hxhim_options_destroy(&info->opts);
    fprintf(stderr, "%d %s %p\n", __LINE__, __func__, file);
    return 0;
}
