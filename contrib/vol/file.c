#include "file.h"

/* H5F routines */
void *H5VL_hxhim_file_create(const char *name, unsigned flags, hid_t fcpl_id,
                             hid_t fapl_id, hid_t dxpl_id, void **req){
    /* struct info_t info; */
    /* if (H5Pget_vol_info(fapl_id, (void **)&info) < 0) { */
    /*     fprintf(stderr, "could not get info\n"); */
    /*     return NULL; */
    /* } */

    /* FILE * f = fopen(name, "wb+"); */
    fprintf(stderr, "%d %s %p\n", __LINE__, __func__, name);
    /* return f; */
    return NULL;
}

void *H5VL_hxhim_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req){
    struct file_info_t info;
    if (H5Pget_vol_info(fapl_id, (void **)&info) < 0) {
        fprintf(stderr, "could not get info\n");
        return NULL;
    }

    /* FILE * f = fopen(name, "wb+"); */
    fprintf(stderr, "%d %s %p\n", __LINE__, __func__, name);
    /* return f; */
    return NULL;
}

herr_t H5VL_hxhim_file_close(void *file, hid_t dxpl_id, void **req){
    fclose(file);
    fprintf(stderr, "%d %s %p\n", __LINE__, __func__, file);
    return 0;
}
