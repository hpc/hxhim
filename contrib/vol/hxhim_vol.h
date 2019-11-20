#ifndef HXHIM_HDF5_VOL_CONNECTOR_H
#define HXHIM_HDF5_VOL_CONNECTOR_H

#include <hdf5.h>

#include "hxhim/hxhim.h"
#include "structs.h"

#define HXHIM_VOL_CONNECTOR_VALUE 12345
#define HXHIM_VOL_CONNECTOR_NAME "hxhim"

H5PL_type_t H5PLget_plugin_type(void);
const void *H5PLget_plugin_info(void);

#endif
