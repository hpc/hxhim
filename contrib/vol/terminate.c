#include "terminate.h"

#include <mpi.h>

#include "hxhim/hxhim.h"

herr_t H5VL_hxhim_terminate(void) {
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
    fprintf(stderr, "%d %s\n", __LINE__, __func__);
    return 0;
}
