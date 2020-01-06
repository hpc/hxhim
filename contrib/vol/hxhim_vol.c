#include "hxhim_vol.h"

#include <stdlib.h>

#include "init_term.h"
#include "info.h"
#include "dataset.h"
#include "file.h"
#include "group.h"

static const H5VL_class_t hxhim_vol_g = {
    0,                                              /* version      */
    (H5VL_class_value_t)HXHIM_VOL_CONNECTOR_VALUE,  /* value        */
    HXHIM_VOL_CONNECTOR_NAME,                       /* name         */
    0,                                              /* capability flags */
    H5VL_hxhim_initialize,                          /* initialize   */
    H5VL_hxhim_terminate,                           /* terminate    */

    {   /* info_cls */
        sizeof(struct file_info_t),                 /* size    */
        H5VL_hxhim_info_copy,                       /* copy    */
        NULL,                                       /* compare */
        H5VL_hxhim_info_free,                       /* free    */
        NULL,                                       /* to_str  */
        NULL,                                       /* from_str */
    },
    {   /* wrap_cls */
        NULL,                                       /* get_object   */
        NULL,                                       /* get_wrap_ctx */
        NULL,                                       /* wrap_object  */
        NULL,                                       /* unwrap_object */
        NULL,                                       /* free_wrap_ctx */
    },
    {   /* attribute_cls */
        NULL,                                       /* create       */
        NULL,                                       /* open         */
        NULL,                                       /* read         */
        NULL,                                       /* write        */
        NULL,                                       /* get          */
        NULL,                                       /* specific     */
        NULL,                                       /* optional     */
        NULL                                        /* close        */
    },
    {   /* dataset_cls */
        H5VL_hxhim_dataset_create,                  /* create       */
        H5VL_hxhim_dataset_open,                    /* open         */
        H5VL_hxhim_dataset_read,                    /* read         */
        H5VL_hxhim_dataset_write,                   /* write        */
        H5VL_hxhim_dataset_get,                     /* get          */
        NULL,                                       /* specific     */
        NULL,                                       /* optional     */
        H5VL_hxhim_dataset_close                    /* close        */
    },
    {   /* datatype_cls */
        NULL,                                       /* commit       */
        NULL,                                       /* open         */
        NULL,                                       /* get_size     */
        NULL,                                       /* specific     */
        NULL,                                       /* optional     */
        NULL                                        /* close        */
    },
    {   /* file_cls */
        H5VL_hxhim_file_create,                     /* create       */
        H5VL_hxhim_file_open,                       /* open         */
        NULL,                                       /* get          */
        H5VL_hxhim_file_specific,                   /* specific     */
        NULL,                                       /* optional     */
        H5VL_hxhim_file_close                       /* close        */
    },
    {   /* group_cls */
        /* (noops)   */
        H5VL_hxhim_group_create,                    /* create       */
        H5VL_hxhim_group_open,                      /* open         */
        H5VL_hxhim_group_get,                       /* get          */
        H5VL_hxhim_group_specific,                  /* specific     */
        H5VL_hxhim_group_optional,                  /* optional     */
        H5VL_hxhim_group_close                      /* close        */
    },
    {   /* link_cls */
        NULL,                                       /* create       */
        NULL,                                       /* copy         */
        NULL,                                       /* move         */
        NULL,                                       /* get          */
        NULL,                                       /* specific     */
        NULL                                        /* optional     */
    },
    {   /* object_cls */
        NULL,                                       /* open         */
        NULL,                                       /* copy         */
        NULL,                                       /* get          */
        NULL,                                       /* specific     */
        NULL                                        /* optional     */
    },
    {   /* request_cls */
        NULL,                                       /* wait         */
        NULL,                                       /* notify       */
        NULL,                                       /* cancel       */
        NULL,                                       /* specific     */
        NULL,                                       /* optional     */
        NULL                                        /* free         */
    },
    NULL                                            /* optional     */
};

H5PL_type_t
H5PLget_plugin_type(void) {
    return H5PL_TYPE_VOL;
}

const void *
H5PLget_plugin_info(void) {
    return &hxhim_vol_g;
}

hid_t hxhim_vol_init(MPI_Comm comm, const int comm_dup) {
    struct under_info_t * under_info = malloc(sizeof(struct under_info_t));

    if (!under_info) {
        fprintf(stderr, "%4d %s failed to malloc under_info\n", __LINE__, __func__);
        return -1;
    }

    if ((under_info->comm_dup = comm_dup)) {
        MPI_Comm_dup(comm, &under_info->comm);
    }
    else {
        under_info->comm = comm;
    }

    /* fill in hxhim options */
    if (hxhim_default_config_reader(&under_info->opts, under_info->comm) != HXHIM_SUCCESS) {
        fprintf(stderr, "%4d %s fill_options\n", __LINE__, __func__);
        hxhim_options_destroy(&under_info->opts);
        if (under_info->comm_dup) {
            MPI_Comm_free(&under_info->comm);
        }
        free(under_info);
        return -1;
    }

    /* start up hxhim */
    if (hxhimOpen(&under_info->hx, &under_info->opts) != HXHIM_SUCCESS) {
        fprintf(stderr, "%4d %s hxhimOpen error\n", __LINE__, __func__);
        hxhimClose(&under_info->hx);
        hxhim_options_destroy(&under_info->opts);
        if (under_info->comm_dup) {
            MPI_Comm_free(&under_info->comm);
        }
        free(under_info);
        return -1;
    }

    printf("%4d %s            %p\n", __LINE__, __func__, under_info);

    /* register vol */
    hid_t vol_id = H5VLregister_connector(H5PLget_plugin_info(), H5P_DEFAULT);
    if (vol_id < 0) {
        fprintf(stderr, "%4d %s register vol error\n", __LINE__, __func__);
        hxhimClose(&under_info->hx);
        hxhim_options_destroy(&under_info->opts);
        if (under_info->comm_dup) {
            MPI_Comm_free(&under_info->comm);
        }
        H5VLunregister_connector(under_info->id);
        free(under_info);
        return -1;
    }

    under_info->id = vol_id;

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_vol(fapl, vol_id, under_info);

    printf("%4d %s            vol %d\n", __LINE__, __func__, fapl);
    return fapl;
}

int hxhim_vol_finalize(hid_t fapl) {
    struct under_info_t * under_info = NULL;
    if (H5Pget_vol_info(fapl, (void **) &under_info) < 0) {
        fprintf(stderr, "could not get info\n");
        return -1;
    }

    MPI_Barrier(under_info->comm);

    H5Pclose(fapl);

    /* unregister vol */
    H5VLunregister_connector(under_info->id);

    /* clean up hxhim */
    hxhimClose(&under_info->hx);
    hxhim_options_destroy(&under_info->opts);
    if (under_info->comm_dup) {
        MPI_Comm_free(&under_info->comm);
    }
    free(under_info);

    return 0;
}
