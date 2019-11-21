#include "info.h"

#include <stdlib.h>
#include <string.h>

void * H5VL_hxhim_info_copy(const void *info){
    const struct under_info_t * old_info = info;
    struct under_info_t * new_info = NULL;
    if (old_info) {
        new_info = malloc(sizeof(struct under_info_t));
        memcpy(new_info, old_info, sizeof(struct under_info_t));
        /* *new_info = * (struct under_info_t *) old_info; */
    }
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    fprintf(stderr, "%4d %s      %p -> %p %d\n", __LINE__, __func__, old_info, new_info, rank);
    return new_info;
}

herr_t H5VL_hxhim_info_free(void *info){
    const struct under_info_t * under_info = info;

    int rank;
    MPI_Comm_rank(under_info->comm, &rank);

    free(info);
    fprintf(stderr, "%4d %s      %p %d\n", __LINE__, __func__, info, rank);
    return 0;
}
