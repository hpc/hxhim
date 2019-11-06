#ifndef HXHIM_HXF5_VOL_FILE_H
#define HXHIM_HXF5_VOL_FILE_H

#include "hxhim_vol.h"

/* H5F routines */
void *H5VL_hxhim_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req);
void *H5VL_hxhim_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req);
herr_t H5VL_hxhim_file_close(void *file, hid_t dxpl_id, void **req);

#endif
