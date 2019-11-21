#ifndef HXHIM_HDF5_VOL_CONNECTOR_H
#define HXHIM_HDF5_VOL_CONNECTOR_H

#include <hdf5.h>

#include "hxhim/hxhim.h"
#include "structs.h"

#define HXHIM_VOL_CONNECTOR_VALUE 12345
#define HXHIM_VOL_CONNECTOR_NAME "hxhim"

H5PL_type_t H5PLget_plugin_type(void);
const void *H5PLget_plugin_info(void);

/* call this function to start the hxhim VOL */
/* MPI should have been initialized prior to */
/* calling this function                     */
hid_t hxhim_vol_init(const char *name, MPI_Comm comm, const int comm_dup);

/* call this function to stop the hxhim VOL  */
int hxhim_vol_finalize(hid_t fapl);

#endif
