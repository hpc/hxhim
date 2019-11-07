#include "initialize.h"

#include <mpi.h>

#include "hxhim/hxhim.h"

herr_t H5VL_hxhim_initialize(hid_t vipl_id) {
    MPI_Init(NULL, NULL);
    MPI_Barrier(MPI_COMM_WORLD);
    fprintf(stderr, "%d %s\n", __LINE__, __func__);
    return 0;
}
