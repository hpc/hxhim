#include "group.h"

void *H5VL_hxhim_group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                              hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req) {
    fprintf(stderr, "%4d %s   %p %s\n", __LINE__, __func__, obj, name);
    return obj;
}

void *H5VL_hxhim_group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                            hid_t gapl_id, hid_t dxpl_id, void **req) {
    fprintf(stderr, "%4d %s    %p %s\n", __LINE__, __func__, obj, name);
    return obj;
}

herr_t H5VL_hxhim_group_get(void *obj, H5VL_group_get_t get_type, hid_t dxpl_id, void **req, va_list arguments) {
    fprintf(stderr, "%4d %s    %p\n", __LINE__, __func__, obj);
    return 0;
}

herr_t H5VL_hxhim_group_specific(void *obj, H5VL_group_specific_t specific_type,
                                 hid_t dxpl_id, void **req, va_list arguments) {
    fprintf(stderr, "%4d %s    %p\n", __LINE__, __func__, obj);
    return 0;
}

herr_t H5VL_hxhim_group_optional(void *obj, hid_t dxpl_id, void **req, va_list arguments) {
    fprintf(stderr, "%4d %s    %p\n", __LINE__, __func__, obj);
    return 0;
}

herr_t H5VL_hxhim_group_close(void *grp, hid_t dxpl_id, void **req) {
    fprintf(stderr, "%4d %s    %p\n", __LINE__, __func__, grp);
    return 0;
}
