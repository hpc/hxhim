#include "dataset.h"
#include "utils/elen.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int find_components(struct dataset_info_t * dataset_info, const char * name, const char sep) {
    if (!dataset_info || !name) {
        return -1;
    }

    const size_t len = strlen(name);

    /* ignore trailing separators */
    size_t i = len;
    while (i && (name[i] == sep)) {
        i--;
    }

    if (!i) {
        return -1;
    }

    /* find next separator */
    while (i && (name[i] != sep)) {
        i--;
    }

    /* copy out the group name */
    if (!(dataset_info->group = malloc(i + 1))) {
        return -1;
    }
    dataset_info->group_len = i;
    memcpy(dataset_info->group, name, dataset_info->group_len);
    dataset_info->group[dataset_info->group_len] = '\0';

    /* copy out the dataset name */
    i++;
    if (!(dataset_info->dataset = malloc(len - i + 1))) {
        return -1;
    }
    dataset_info->dataset_len = len - i;
    memcpy(dataset_info->dataset, &name[i], dataset_info->dataset_len);

    return 0;
}

/* H5D routines */
void *H5VL_hxhim_dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id,
                                hid_t dapl_id, hid_t dxpl_id, void **req){
    struct file_info_t * file_info = obj;

    /* store values into state variable that is passed around */
    struct dataset_info_t * dataset_info = malloc(sizeof(struct dataset_info_t));
    dataset_info->file = obj;
    find_components(dataset_info, name, '/');
    dataset_info->lcpl_id = lcpl_id;
    dataset_info->type_id = type_id;
    dataset_info->space_id = space_id;
    dataset_info->dcpl_id = dcpl_id;
    dataset_info->dapl_id = dapl_id;
    dataset_info->dxpl_id = dxpl_id;

    /* dataset_info->type_id = type_id; */
    dataset_info->space_id = space_id;

    const hsize_t dims = H5Sget_simple_extent_ndims(space_id);
    hsize_t * maxdims = malloc(dims * sizeof(hsize_t));
    H5Sget_simple_extent_dims(space_id, NULL, maxdims);

    fprintf(stderr, "%4d %s %p %s ", __LINE__, __func__, dataset_info, name);

    switch (H5Tget_class(type_id)) {
        case H5T_INTEGER:
            fprintf(stderr, "H5T_INTEGER");
            break;
        case H5T_FLOAT:
            fprintf(stderr, "H5T_FLOAT");
            break;
        case H5T_STRING:
            fprintf(stderr, "H5T_STRING");
            break;
        case H5T_BITFIELD:
            fprintf(stderr, "H5T_BITFIELD");
            break;
        case H5T_OPAQUE:
            fprintf(stderr, "H5T_OPAQUE");
            break;
        case H5T_COMPOUND:
            fprintf(stderr, "H5T_COMPOUND");
            break;
        case H5T_REFERENCE:
            fprintf(stderr, "H5T_REFERENCE");
            break;
        case H5T_ENUM:
            fprintf(stderr, "H5T_ENUM");
            break;
        case H5T_VLEN:
            fprintf(stderr, "H5T_VLEN");
            break;
        case H5T_ARRAY:
            fprintf(stderr, "H5T_ARRAY");
            break;
        default:
            fprintf(stderr, "other class");
            break;
    }

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
    find_components(dataset_info, name, '/');

    fprintf(stderr, "%4d %s   %p %s %p %d\n", __LINE__, __func__, obj, name, dataset_info, dataset_info->file->under_vol->id);
    return dataset_info;
}

herr_t H5VL_hxhim_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
                               hid_t xfer_plist_id, void * buf, void **req){
    fprintf(stderr, "%4d %s   %p buf: %p\n", __LINE__, __func__, dset, buf);
    struct dataset_info_t * dataset_info = dset;
    const size_t size = H5Tget_size(mem_type_id);

    /* if (mem_space_id == H5S_ALL) { */
    /*     fprintf(stderr, "cannot extract values from mem space with H5S_ALL\n"); */
    /*     return -1; */
    /* } */
    /* if (file_space_id == H5S_ALL) { */
    /*     fprintf(stderr, "cannot extract values from file space with H5S_ALL\n"); */
    /*     return -1; */
    /* } */
    /* { */
    /*     /\* const hssize_t count = H5Sget_select_npoints(mem_space_id); *\/ */
    /*     const htri_t is_simple = H5Sis_simple(mem_space_id); */
    /*     const int ranks = H5Sget_simple_extent_ndims(mem_space_id); */

    /*     hsize_t * dims = malloc(ranks * sizeof(hsize_t)); */
    /*     H5Sget_simple_extent_dims(mem_space_id, dims, NULL); */

    /*     hsize_t * start = malloc(ranks * sizeof(hsize_t)); */
    /*     hsize_t * end = malloc(ranks * sizeof(hsize_t)); */
    /*     H5Sget_select_bounds(mem_space_id, start, end); */
    /*     fprintf(stderr, "%4d %s   %p %d %d (%zu x %zu) [%zu %zu]\n", __LINE__, __func__, dset, H5S_ALL, mem_space_id, size, dims[0], start[0], end[0]); */
    /*     free(end); */
    /*     free(start); */
    /*     free(dims); */
    /* } */
    /* { */
    /*     /\* const hssize_t count = H5Sget_select_npoints(file_space_id); *\/ */
    /*     const htri_t is_simple = H5Sis_simple(file_space_id); */
    /*     const int ranks = H5Sget_simple_extent_ndims(file_space_id); */

    /*     hsize_t * dims = malloc(ranks * sizeof(hsize_t)); */
    /*     H5Sget_simple_extent_dims(file_space_id, dims, NULL); */

    /*     hsize_t * start = malloc(ranks * sizeof(hsize_t)); */
    /*     hsize_t * end = malloc(ranks * sizeof(hsize_t)); */
    /*     H5Sget_select_bounds(file_space_id, start, end); */
    /*     fprintf(stderr, "%4d %s   %p %d %d (%zu x %zu) [%zu %zu]\n", __LINE__, __func__, dset, H5S_ALL, file_space_id, size, dims[0], start[0], end[0]); */
    /*     free(end); */
    /*     free(start); */
    /*     free(dims); */
    /* } */

    hxhimGet(&dataset_info->file->hx,
             dataset_info->group, dataset_info->group_len,
             dataset_info->dataset, dataset_info->dataset_len,
             HXHIM_BYTE_TYPE);

    hxhim_results_t *res = hxhimFlush(&dataset_info->file->hx);
    hxhim_results_goto_head(res);
    for(hxhim_results_goto_head(res); hxhim_results_valid(res) == HXHIM_SUCCESS; hxhim_results_goto_next(res)) {
        hxhim_result_type_t type;
        hxhim_results_type(res, &type);
        switch (type) {
            case HXHIM_RESULT_PUT:
                break;
            case HXHIM_RESULT_GET:
                {
                    void * object = NULL;
                    size_t object_len = 0;
                    hxhim_results_get_object(res, &object, &object_len);
                    memcpy(buf, object, object_len);
                }
                break;
            case HXHIM_RESULT_DEL:
                break;
            default:
                break;
        }
    }
    hxhim_results_destroy(&dataset_info->file->hx, res);

    return 0;
}

herr_t H5VL_hxhim_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
                                hid_t xfer_plist_id, const void * buf, void **req){
    struct dataset_info_t * dataset_info = dset;
    const size_t size = H5Tget_size(mem_type_id);
    const hssize_t count = H5Sget_select_npoints(dataset_info->space_id);
    fprintf(stderr, "%4d %s  %p [%s] [%s] size: %zu count: %lld\n", __LINE__, __func__, dset, dataset_info->group, dataset_info->dataset, size, count);

    hxhimPut(&dataset_info->file->hx,
             dataset_info->group, strlen(dataset_info->group),
             dataset_info->dataset, strlen(dataset_info->dataset),
             HXHIM_BYTE_TYPE, (void *) buf, size * count);

    hxhim_results_t *put_results = hxhimFlushPuts(&dataset_info->file->hx);
    if (!put_results) {
        fprintf(stderr, "failed to put\n");
    }
    hxhim_results_destroy(&dataset_info->file->hx, put_results);

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
                if ((*ret_id = H5Pcopy(dataset_info->dapl_id)) < 0) {
                    fprintf(stderr, "can't get dapl\n");
                    ret = -1;
                }
            }
            break;
        case H5VL_DATASET_GET_DCPL:                  /* creation property list              */
            {
                fprintf(stderr, "         get dcpl\n");
                hid_t *ret_id = va_arg(arguments, hid_t *);
                if ((*ret_id = H5Pcopy(dataset_info->dcpl_id)) < 0) {
                    fprintf(stderr, "can't get dcpl\n");
                    ret = -1;
                }
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
                if ((*ret_id = H5Pcopy(dataset_info->space_id)) < 0) {
                    fprintf(stderr, "can't get space\n");
                    ret = -1;
                }
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
                if ((*ret_id = H5Pcopy(dataset_info->type_id)) < 0) {
                    fprintf(stderr, "can't get type\n");
                    ret = -1;
                }
            }
            break;
    }

    return ret;
}

herr_t H5VL_hxhim_dataset_close(void *dset, hid_t dxpl_id, void **req){
    struct dataset_info_t * dataset_info = dset;
    free(dataset_info->dataset);
    free(dataset_info->group);
    free(dataset_info);

    fprintf(stderr, "%4d %s  %p\n", __LINE__, __func__, dset);
    return 0;
}
