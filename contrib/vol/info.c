#include "info.h"

#include <stdlib.h>
#include <string.h>

void * H5VL_hxhim_info_copy(const void *info){
    const struct info_t * old_info = info;
    struct info_t * new_info = malloc(sizeof(struct file_info_t));
    memcpy(new_info, old_info, sizeof(struct file_info_t));
    return new_info;
}

herr_t H5VL_hxhim_info_free(void *info){
    free(info);
    fprintf(stderr, "%d %s %p\n", __LINE__, __func__, info);
    return 0;
}
