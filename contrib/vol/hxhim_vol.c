#include "hxhim_vol.h"

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
