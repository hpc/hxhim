#include "init_term.h"

#include <mpi.h>

#include "hxhim/hxhim.h"

/* whether or not MPI has already been initialized */
static int preinitialized = 0;

herr_t H5VL_hxhim_initialize(hid_t vipl_id) {
    MPI_Initialized(&preinitialized);

    int provided = -1;
    if (preinitialized) {
        MPI_Query_thread(&provided);
    }
    /* only run MPI_Init_thread if MPI wasn't initialized by the calling process */
    else {
        MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided);
    }

    /* make sure MPI_THREAD_MULTIPLE was provided */
    if (provided != MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "%4d %s no MPI_THREAD_MULTIPLE\n", __LINE__, __func__);
        MPI_Finalize();
    }

    MPI_Barrier(MPI_COMM_WORLD);

    fprintf(stderr, "%4d %s %d\n", __LINE__, __func__, preinitialized);
    return 0;
}

herr_t H5VL_hxhim_terminate(void) {
    MPI_Barrier(MPI_COMM_WORLD);

    /* only finalize MPI if MPI wasn't initialized by the calling process */
    if (!preinitialized) {
        MPI_Finalize();
    }

    fprintf(stderr, "%4d %s\n", __LINE__, __func__);
    return 0;
}
