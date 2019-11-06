#ifndef HXHIM_HXF5_VOL_INFO_H
#define HXHIM_HXF5_VOL_INFO_H

#include "hxhim_vol.h"

/* VOL connector info fields & callbacks */
void * H5VL_hxhim_info_copy(const void *info);
herr_t H5VL_hxhim_info_free(void *info);

#endif
