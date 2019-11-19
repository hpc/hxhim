#include "dataset.h"
#include "utils/elen.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char PREFIX[] = "HDF5_DS:";
static size_t PREFIX_LEN = (sizeof(PREFIX) - 1);

const char * get_type_name(hid_t type_id) {
    char * out = NULL;
    switch (H5Tget_class(type_id)) {
        case H5T_INTEGER:
            out = "H5T_INTEGER";
            break;
        case H5T_FLOAT:
            out = "H5T_FLOAT";
            break;
        case H5T_STRING:
            out = "H5T_STRING";
            break;
        case H5T_BITFIELD:
            out = "H5T_BITFIELD";
            break;
        case H5T_OPAQUE:
            out = "H5T_OPAQUE";
            break;
        case H5T_COMPOUND:
            out = "H5T_COMPOUND";
            break;
        case H5T_REFERENCE:
            out = "H5T_REFERENCE";
            break;
        case H5T_ENUM:
            out = "H5T_ENUM";
            break;
        case H5T_VLEN:
            out = "H5T_VLEN";
            break;
        case H5T_ARRAY:
            out = "H5T_ARRAY";
            break;
        default:
            out = "other class";
            break;
    }
    return out;
}

void create_subject(const char * name, char ** subject, size_t * len) {
    *len = PREFIX_LEN + strlen(name);
    *subject = malloc(*len + 1);
    snprintf(*subject, *len + 1, "%s%s", PREFIX, name);
}

/* H5D routines */
void *H5VL_hxhim_dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id,
                                hid_t dapl_id, hid_t dxpl_id, void **req){
    struct file_info_t * file_info = obj;

    /* store values into state variable that is passed around */
    struct dataset_info_t * dataset_info = malloc(sizeof(struct dataset_info_t));
    dataset_info->file = obj;
    create_subject(name, &dataset_info->subject, &dataset_info->subject_len);

    const hsize_t dims = H5Sget_simple_extent_ndims(space_id);
    hsize_t * maxdims = malloc(dims * sizeof(hsize_t));
    H5Sget_simple_extent_dims(space_id, NULL, maxdims);

    fprintf(stderr, "%4d %s %p %s %s ", __LINE__, __func__, dataset_info, name, get_type_name(type_id));
    fprintf(stderr, " (%zu bytes) dims: %lld [ ", H5Tget_size(type_id), dims);
    for(hsize_t i = 0; i < dims; i++) {
        fprintf(stderr, "%lld ", maxdims[i]);
    }
    fprintf(stderr, "]\n");
    free(maxdims);

    return dataset_info;
}

void *H5VL_hxhim_dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                              hid_t dapl_id, hid_t dxpl_id, void **req){
    struct dataset_info_t * dataset_info = malloc(sizeof(struct dataset_info_t));
    dataset_info->file = obj;
    create_subject(name, &dataset_info->subject, &dataset_info->subject_len);

    fprintf(stderr, "%4d %s   %p %s %p\n", __LINE__, __func__, obj, name, dataset_info);
    return dataset_info;
}

herr_t H5VL_hxhim_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
                               hid_t xfer_plist_id, void * buf, void **req){
    struct dataset_info_t * dataset_info = dset;
    /* const size_t size = H5Tget_size(mem_type_id); */
    /* const hssize_t count = H5Sget_select_npoints(file_space_id); */

    /* { */
    /*     /\* const hssize_t count = H5Sget_select_npoints(file_space_id); *\/ */
    /*     const htri_t is_simple = H5Sis_simple(file_space_id); */
    /*     const int ranks = H5Sget_simple_extent_ndims(file_space_id); */

    /*     hsize_t * dims = malloc(ranks * sizeof(hsize_t)); */
    /*     H5Sget_simple_extent_dims(file_space_id, dims, NULL); */

    /*     hsize_t * start = malloc(ranks * sizeof(hsize_t)); */
    /*     hsize_t * end = malloc(ranks * sizeof(hsize_t)); */
    /*     H5Sget_select_bounds(file_space_id, start, end); */
    /*     fprintf(stderr, "%4d %s   %p buf: %p %d %d (%zu x %zu) [%zu %zu]\n", __LINE__, __func__, dset, buf, H5S_ALL, file_space_id, size, dims[0], start[0], end[0]); */
    /*     free(end); */
    /*     free(start); */
    /*     free(dims); */
    /* } */

    const char * predicate = get_type_name(mem_type_id);
    const size_t predicate_len = strlen(predicate);

    hxhimGet(&dataset_info->file->hx,
             dataset_info->subject, dataset_info->subject_len,
             (void *) predicate, predicate_len,
             HXHIM_BYTE_TYPE);

    hxhim_results_t *res = hxhimFlushGets(&dataset_info->file->hx);
    for(hxhim_results_goto_head(res); hxhim_results_valid(res) == HXHIM_SUCCESS; hxhim_results_goto_next(res)) {
        hxhim_result_type_t type;
        hxhim_results_type(res, &type);
        switch (type) {
            case HXHIM_RESULT_GET:
                {
                    void * object = NULL;
                    size_t object_len = 0;
                    const int rc = hxhim_results_get_object(res, &object, &object_len);

                    /*
                       cannot read directly into buf
                       - need to cut data for hyperslabs
                       - hxhim allocates memory from its buffers, not from global memory space
                           - destroyed when hxhim_results_destroy is called
                           - if not destroyed, user would have to pass in double pointer to buf, and manually clean up
                    */

                    if (file_space_id == H5S_ALL) {
                        memcpy(buf, object, object_len);
                    }
                    else {
                        /* process hyperslab */
                    }
                }
                break;
            default:
                break;
        }
    }
    hxhim_results_destroy(&dataset_info->file->hx, res);
    fprintf(stderr, "%4d %s   %p (%s, %s) -> %p %d %d %p\n", __LINE__, __func__, dset, dataset_info->subject, predicate, buf, mem_space_id, file_space_id, req);

    return 0;
}

herr_t H5VL_hxhim_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
                                hid_t xfer_plist_id, const void * buf, void **req){
    /* if (mem_space_id == H5S_ALL) { */
    /*     fprintf(stderr, "%d %s cannot write values to mem space with H5S_ALL\n", __LINE__, __func__); */
    /*     return -1; */
    /* } */

    if (file_space_id == H5S_ALL) {
        fprintf(stderr, "%d %s cannot write values to file space with H5S_ALL\n", __LINE__, __func__);
        return -1;
    }

    struct dataset_info_t * dataset_info = dset;
    const size_t size = H5Tget_size(mem_type_id);
    const hssize_t count = H5Sget_select_npoints(file_space_id);

    const char * predicate = get_type_name(mem_type_id);
    const size_t predicate_len = strlen(predicate);

    hxhimPut(&dataset_info->file->hx,
             dataset_info->subject, dataset_info->subject_len,
             (void *) predicate, predicate_len,
             HXHIM_BYTE_TYPE, (void *) buf, size * count);

    /* hxhim_results_t *put_results = hxhimFlushPuts(&dataset_info->file->hx); */
    /* if (!put_results) { */
    /*     fprintf(stderr, "failed to put\n"); */
    /* } */
    /* hxhim_results_destroy(&dataset_info->file->hx, put_results); */

    fprintf(stderr, "%4d %s  %p (%s, %s, %p) size: %zu count: %lld\n", __LINE__, __func__, dset, dataset_info->subject, predicate, buf, size, count);

    return 0;
}

herr_t H5VL_hxhim_dataset_get(void *obj, H5VL_dataset_get_t get_type, hid_t dxpl_id,
                              void **req, va_list arguments) {
    fprintf(stderr, "%4d %s %p\n", __LINE__, __func__, obj);

    struct dataset_info_t * dataset_info = obj;
    herr_t ret = 0;

    switch (get_type) {
        case H5VL_DATASET_GET_DAPL:                  /* access property list                */
            {
                fprintf(stderr, "         get dapl\n");
                hid_t *ret_id = va_arg(arguments, hid_t *);
                /* if ((*ret_id = H5Pcopy(dataset_info->dapl_id)) < 0) { */
                /*     fprintf(stderr, "can't get dapl\n"); */
                /*     ret = -1; */
                /* } */
            }
            break;
        case H5VL_DATASET_GET_DCPL:                  /* creation property list              */
            {
                fprintf(stderr, "         get dcpl\n");
                hid_t *ret_id = va_arg(arguments, hid_t *);
                /* if ((*ret_id = H5Pcopy(dataset_info->dcpl_id)) < 0) { */
                /*     fprintf(stderr, "can't get dcpl\n"); */
                /*     ret = -1; */
                /* } */
            }
            break;
        case H5VL_DATASET_GET_OFFSET:                /* offset                              */
            {
                fprintf(stderr, "         get offset\n");
                hid_t *ret_id = va_arg(arguments, hid_t *);
            }
            break;
        case H5VL_DATASET_GET_SPACE:                 /* dataspace                           */
            {
                fprintf(stderr, "         get space\n");
                hid_t *ret_id = va_arg(arguments, hid_t *);
                /* if ((*ret_id = H5Pcopy(dataset_info->space_id)) < 0) { */
                /*     fprintf(stderr, "can't get space\n"); */
                /*     ret = -1; */
                /* } */
            }
            break;
        case H5VL_DATASET_GET_SPACE_STATUS:          /* space status                        */
            {
                fprintf(stderr, "         get space status\n");
                H5D_space_status_t *ret_id = va_arg(arguments, H5D_space_status_t *);
            }
            break;
        case H5VL_DATASET_GET_STORAGE_SIZE:          /* storage size                        */
            {
                fprintf(stderr, "         get storage size\n");
                hsize_t *ret_id = va_arg(arguments, hsize_t *);
            }
            break;
        case H5VL_DATASET_GET_TYPE:                  /* datatype                            */
            {
                fprintf(stderr, "         get type\n");
                hid_t *ret_id = va_arg(arguments, hid_t *);
                /* if ((*ret_id = H5Pcopy(dataset_info->type_id)) < 0) { */
                /*     fprintf(stderr, "can't get type\n"); */
                /*     ret = -1; */
                /* } */
            }
            break;
    }

    return ret;
}

herr_t H5VL_hxhim_dataset_close(void *dset, hid_t dxpl_id, void **req){
    struct dataset_info_t * dataset_info = dset;
    free(dataset_info->subject);
    free(dataset_info);

    fprintf(stderr, "%4d %s  %p\n", __LINE__, __func__, dset);
    return 0;
}
