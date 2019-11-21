#include "init_term.h"

#include <mpi.h>

#include "hxhim/hxhim.h"

herr_t H5VL_hxhim_initialize(hid_t vipl_id) {
    fprintf(stderr, "%4d %s\n", __LINE__, __func__);
    return 0;
}

herr_t H5VL_hxhim_terminate(void) {
    fprintf(stderr, "%4d %s\n", __LINE__, __func__);
    return 0;
}
