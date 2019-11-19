#ifndef HXHIM_HXF5_VOL_GROUP_H
#define HXHIM_HXF5_VOL_GROUP_H

#include "hxhim_vol.h"

void *H5VL_hxhim_group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                              hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req);
void *H5VL_hxhim_group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                            hid_t gapl_id, hid_t dxpl_id, void **req);
herr_t H5VL_hxhim_group_get(void *obj, H5VL_group_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
herr_t H5VL_hxhim_group_specific(void *obj, H5VL_group_specific_t specific_type,
                                 hid_t dxpl_id, void **req, va_list arguments);
herr_t H5VL_hxhim_group_optional(void *obj, hid_t dxpl_id, void **req, va_list arguments);
herr_t H5VL_hxhim_group_close(void *grp, hid_t dxpl_id, void **req);

#endif
