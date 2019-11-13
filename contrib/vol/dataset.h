#ifndef HXHIM_HXF5_VOL_DATASET_H
#define HXHIM_HXF5_VOL_DATASET_H

#include "hxhim_vol.h"
#include "structs.h"

/* H5D routines */
void *H5VL_hxhim_dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                              hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id,
                              hid_t dapl_id, hid_t dxpl_id, void **req);
void *H5VL_hxhim_dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                            hid_t dapl_id, hid_t dxpl_id, void **req);
herr_t H5VL_hxhim_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
                             hid_t xfer_plist_id, void * buf, void **req);
herr_t H5VL_hxhim_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
                              hid_t xfer_plist_id, const void * buf, void **req);
herr_t H5VL_hxhim_dataset_get(void *obj, H5VL_dataset_get_t get_type, hid_t dxpl_id,
                              void **req, va_list arguments);
herr_t H5VL_hxhim_dataset_close(void *dset, hid_t dxpl_id, void **req);

#endif
