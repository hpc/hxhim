#include "file.h"

#include <stdlib.h>
#include <string.h>

static struct file_info_t * H5VL_hxhim_file(const char * name, struct under_info_t * under_info, unsigned flags, hid_t fapl_id) {
    /* store values into state variable that is passed around */
    struct file_info_t * file_info = malloc(sizeof(struct file_info_t));

    const size_t len = strlen(name) + 1;
    file_info->name = malloc(len);
    memcpy(file_info->name, name, len);
    file_info->under_vol = under_info;
    file_info->flags = flags;
    file_info->fapl_id = fapl_id;

    return file_info;
}

/* H5F routines */
void *H5VL_hxhim_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req) {
    struct under_info_t * under_info = NULL;
    if (H5Pget_vol_info(fapl_id, (void **)&under_info) < 0) {
        fprintf(stderr, "could not get info\n");
        return NULL;
    }

    struct file_info_t * file_info = H5VL_hxhim_file(name, under_info, flags, fapl_id);
    fprintf(stderr, "%4d %s    %p %s %p\n", __LINE__, __func__, file_info, name, under_info->hx.p);
    return file_info;
}

void *H5VL_hxhim_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req) {
    struct under_info_t * under_info = NULL;
    if (H5Pget_vol_info(fapl_id, (void **)&under_info) < 0) {
        fprintf(stderr, "could not get info\n");
        return NULL;
    }

    void * ret = H5VL_hxhim_file(name, under_info, flags, fapl_id);
    fprintf(stderr, "%4d %s      %p %s\n", __LINE__, __func__, ret, name);
    return ret;
}

herr_t H5VL_hxhim_file_specific(void *obj, H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments) {
    fprintf(stderr, "%4d %s  %p\n", __LINE__, __func__, obj);

    struct file_info_t * file_info = obj;

    int rc = 0;

    switch (specific_type) {
        /* case H5VL_FILE_POST_OPEN:                    /\* Adjust file after open: with wrapping context *\/ */
        /*     fprintf(stderr, "         post open\n"); */
        /*     break; */
        case H5VL_FILE_FLUSH:                        /* Flush file                       */
            fprintf(stderr, "         flush\n");
            hxhim_results_t *res = hxhimFlush(&file_info->under_vol->hx);
            if (!res) {
                fprintf(stderr, "Could not flush HXHIM context\n");
                rc = -1;
            }

            for(hxhim_results_goto_head(res); hxhim_results_valid(res) == HXHIM_SUCCESS; hxhim_results_goto_next(res)) {
                hxhim_result_type_t type;
                hxhim_result_type(res, &type);
                switch (type) {
                    case HXHIM_RESULT_PUT:
                    case HXHIM_RESULT_DEL:
                        /* user is responsible for subject and predicate, so don't do anything */
                        break;
                    case HXHIM_RESULT_GET:
                        {
                            int status = 0;
                            hxhim_result_status(res, &status);
                            if (status != HXHIM_SUCCESS) {
                                fprintf(stderr, "GET error\n");
                                rc = -1;
                                continue;
                            }

                            void *object = NULL;
                            size_t object_len = 0;
                            hxhim_result_object(res, &object, &object_len);
                            /* need to copy object to somewhere */
                            /* memcpy(dst, object, object_len); */
                        }
                        break;
                    default:
                        break;
                }
            }
            hxhim_results_destroy(res);

            break;
        case H5VL_FILE_REOPEN:                       /* Reopen the file                  */
            fprintf(stderr, "         reopen\n");
            /* do not close original file */
            fprintf(stderr, "cannot have the same hxhim data opened multiple times simultaneously\n");
            rc = -1;
            break;
        case H5VL_FILE_MOUNT:                        /* Mount a file                     */
            fprintf(stderr, "         mount\n");
            break;
        case H5VL_FILE_UNMOUNT:                      /* Unmount a file                   */
            fprintf(stderr, "         unmount\n");
            break;
        case H5VL_FILE_IS_ACCESSIBLE:                /* Check if a file is accessible    */
            fprintf(stderr, "         is_accessible\n");
            break;
        case H5VL_FILE_DELETE:                       /* Delete a file                    */
            fprintf(stderr, "         delete\n");
            break;
    }

    return rc;
}

herr_t H5VL_hxhim_file_close(void *file, hid_t dxpl_id, void **req) {
    struct file_info_t * file_info = file;
    free(file_info->name);
    free(file_info);
    fprintf(stderr, "%4d %s     %p\n", __LINE__, __func__, file);
    return 0;
}
