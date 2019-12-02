#include "info.h"

#include <stdlib.h>
#include <string.h>

void * H5VL_hxhim_info_copy(const void *info){
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    fprintf(stderr, "%4d %s      %p %d\n", __LINE__, __func__, info, rank);
    return (void *) info;
}

herr_t H5VL_hxhim_info_free(void *info){
    const struct under_info_t * under_info = (struct under_info_t *) info;

    int rank;
    MPI_Comm_rank(under_info->comm, &rank);
    fprintf(stderr, "%4d %s      %p %d\n", __LINE__, __func__, info, rank);
    return 0;
}
