#include "dataset.h"

#include <stdio.h>
#include <stdlib.h>

/* H5D routines */
void *H5VL_hxhim_dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id,
                                hid_t dapl_id, hid_t dxpl_id, void **req){

    struct file_info_t * file_info = obj;

    struct dataset_info_t * dataset_info = malloc(sizeof(struct dataset_info_t));
    dataset_info->hx = &file_info->hx;
    dataset_info->type_id = type_id;
    dataset_info->space_id = space_id;

    const hsize_t dims = H5Sget_simple_extent_ndims(space_id);
    hsize_t * maxdims = malloc(dims * sizeof(hsize_t));
    H5Sget_simple_extent_dims(space_id, NULL, maxdims);

    fprintf(stderr, "%d %s %p %s ", __LINE__, __func__, dataset_info, name);

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
    fprintf(stderr, "%d %s %s ", __LINE__, __func__, name);
    return NULL;
}

herr_t H5VL_hxhim_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
                               hid_t xfer_plist_id, void * buf, void **req){
    struct dataset_info_t * dataset_info = dset;
    const size_t size = H5Tget_size(mem_type_id);
    const hssize_t count = H5Sget_select_npoints(dataset_info->space_id);
    fprintf(stderr, "%d %s   %p size: %zu count: %lld\n", __LINE__, __func__, dset, size, count);

    /* // Add subject-predicate to get back */
    /* EXPECT_EQ(hxhim::GetDouble(&hx, */
    /*                            (void *)&SUBJECT, sizeof(SUBJECT), */
    /*                            (void *)&PREDICATE, sizeof(PREDICATE)), */
    /*           HXHIM_SUCCESS); */

    /* // Flush all queued items */
    /* hxhim::Results *get_results = hxhim::FlushGets(&hx); */
    /* ASSERT_NE(get_results, nullptr); */

    /* // get the results and compare them with the original data */
    /* EXPECT_EQ(get_results->size(), 1); */
    /* for(get_results->GoToHead(); get_results->Valid(); get_results->GoToNext()) { */
    /*     hxhim::Results::Result *res = get_results->Curr(); */
    /*     ASSERT_NE(res, nullptr); */

    /*     ASSERT_EQ(res->status, HXHIM_SUCCESS); */
    /*     ASSERT_EQ(res->type, HXHIM_RESULT_GET); */

    /*     hxhim::Results::Get *get = static_cast<hxhim::Results::Get *>(get_results->Curr()); */

    /*     Subject_t *subject = (Subject_t *) get->subject; */
    /*     EXPECT_EQ(*subject, SUBJECT); */

    /*     Predicate_t *predicate = (Predicate_t *) get->predicate; */
    /*     EXPECT_EQ(*predicate, PREDICATE); */

    /*     Object_t *object = (Object_t *) get->object; */
    /*     ASSERT_NE(object, nullptr); */
    /*     if (std::is_same<float, Object_t>::value) { */
    /*         EXPECT_FLOAT_EQ(*object, OBJECT); */
    /*     } */
    /*     else if (std::is_same<double, Object_t>::value) { */
    /*         EXPECT_DOUBLE_EQ(*object, OBJECT); */
    /*     } */
    /* } */

    /* hxhim::Results::Destroy(&hx, get_results); */

    /* fseek(info->file, 0, SEEK_SET); */
    /* return fread(buf, size, count, info->file); */
    return 0;
}

herr_t H5VL_hxhim_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
                                hid_t xfer_plist_id, const void * buf, void **req){
    struct dataset_info_t * dataset_info = dset;
    const size_t size = H5Tget_size(mem_type_id);
    const hssize_t count = H5Sget_select_npoints(dataset_info->space_id);
    fprintf(stderr, "%d %s  %p size: %zu count: %lld\n", __LINE__, __func__, dset, size, count);

    const struct float3_t * floats = buf;
    for(hssize_t i = 0; i < count; i++) {
        fprintf(stderr, "    %f %f %f\n", floats[i].subject, floats[i].predicate, floats[i].object);
        if (hxhimPutFloat(dataset_info->hx,
                          (void *)&floats[i].subject, sizeof(floats[i].subject),
                          (void *)&floats[i].predicate, sizeof(floats[i].predicate),
                          (float *)&floats[i].object) != HXHIM_SUCCESS) {
            fprintf(stderr, "failed to put %f %f %f\n", floats[i].subject, floats[i].predicate, floats[i].object);
        }
    }

    // Flush all queued items
    hxhim_results_t *put_results = hxhimFlushPuts(dataset_info->hx);
    if (!put_results) {
        fprintf(stderr, "failed to put\n");
    }
    hxhim_results_destroy(dataset_info->hx, put_results);

    return (uintptr_t) put_results;
}

herr_t H5VL_hxhim_dataset_close(void *dset, hid_t dxpl_id, void **req){
    fprintf(stderr, "%d %s  %p\n", __LINE__, __func__, dset);
    return 0;
}
