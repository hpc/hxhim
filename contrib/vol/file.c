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
            (hxhim_options_set_keys_alloc_size(opts, 64)                          == HXHIM_SUCCESS) &&
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

struct file_info_t * H5VL_hxhim_file(const char * name, struct under_info_t * info, hid_t fapl_id, hid_t dxpl_id) {
    /* store values into state variable that is passed around */
    struct file_info_t * file_info = malloc(sizeof(struct file_info_t));

    if (!fill_options(&file_info->opts, name)) {
        fprintf(stderr, "%4d %s fill_options\n", __LINE__, __func__);
        hxhim_options_destroy(&file_info->opts);
        free(file_info);
        return NULL;
    }

    /* start up hxhim */
    if (hxhimOpen(&file_info->hx, &file_info->opts) != HXHIM_SUCCESS) {
        fprintf(stderr, "%4d %s hxhimOpen error\n", __LINE__, __func__);
        hxhimClose(&file_info->hx);
        hxhim_options_destroy(&file_info->opts);
        free(file_info);
        return NULL;
    }

    file_info->under_vol = info;
    file_info->fapl_id = fapl_id;
    file_info->dxpl_id = dxpl_id;

    return file_info;
}

/* H5F routines */
void *H5VL_hxhim_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req) {
    struct under_info_t * info = NULL;
    if (H5Pget_vol_info(fapl_id, (void **)&info) < 0) {
        fprintf(stderr, "could not get info\n");
        return NULL;
    }

    struct file_info_t * ret = H5VL_hxhim_file(name, info, fapl_id, dxpl_id);
    fprintf(stderr, "%4d %s    %p %s %d\n", __LINE__, __func__, ret, name, info->id);
    return ret;
}

void *H5VL_hxhim_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req) {
    struct under_info_t * info = NULL;
    if (H5Pget_vol_info(fapl_id, (void **)&info) < 0) {
        fprintf(stderr, "could not get info\n");
        return NULL;
    }

    void * ret = H5VL_hxhim_file(name, info, fapl_id, dxpl_id);
    fprintf(stderr, "%4d %s      %p %s %d\n", __LINE__, __func__, ret, name, info->id);
    return ret;
}

herr_t H5VL_hxhim_file_specific(void *obj, H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments) {
    fprintf(stderr, "%4d %s  %p\n", __LINE__, __func__, obj);

    struct file_info_t * file_info = obj;

    switch (specific_type) {
        case H5VL_FILE_POST_OPEN:                    /* Adjust file after open: with wrapping context */
            fprintf(stderr, "         post open\n");
            break;
        case H5VL_FILE_FLUSH:                        /* Flush file                       */
            hxhimFlush(&file_info->hx);
            fprintf(stderr, "         flush\n");
            break;
        case H5VL_FILE_REOPEN:                       /* Reopen the file                  */
            fprintf(stderr, "         reopen\n");
            break;
        case H5VL_FILE_MOUNT:                        /* Mount a file                     */
            fprintf(stderr, "         mount\n");
            break;
        case H5VL_FILE_UNMOUNT:                      /* Unmount a file                   */
            fprintf(stderr, "         unmount\n");
            break;
        case H5VL_FILE_IS_ACCESSIBLE:                /* Check if a file is accessible    */
            fprintf(stderr, "         is_accessible\n");
            break;
        case H5VL_FILE_DELETE:                       /* Delete a file                    */
            fprintf(stderr, "         delete\n");
            break;
    }

    return 0;
}

herr_t H5VL_hxhim_file_close(void *file, hid_t dxpl_id, void **req) {
    struct file_info_t * file_info = file;
    hxhimClose(&file_info->hx);
    hxhim_options_destroy(&file_info->opts);
    free(file_info);
    fprintf(stderr, "%4d %s     %p\n", __LINE__, __func__, file);
    return 0;
}
