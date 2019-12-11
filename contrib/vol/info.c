#include "info.h"

#include <stdlib.h>
#include <string.h>

void * H5VL_hxhim_info_copy(const void *info){
    fprintf(stderr, "%4d %s      %p\n", __LINE__, __func__, info);
    return (void *) info;
}

herr_t H5VL_hxhim_info_free(void *info){
    const struct under_info_t * under_info = (struct under_info_t *) info;
    fprintf(stderr, "%4d %s      %p\n", __LINE__, __func__, info);
    return 0;
}
