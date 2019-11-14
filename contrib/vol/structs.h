#ifndef HXHIM_HXF5_VOL_STRUCTS_H
#define HXHIM_HXF5_VOL_STRUCTS_H

#include <stdio.h>

#include "hxhim_vol.h"

struct under_info_t {
    hid_t id;
};

/* holds arguments converted for use by the file_* callbacks */
struct file_info_t {
    struct under_info_t * under_vol;
    hid_t fapl_id;
    hid_t dxpl_id;

    hxhim_options_t opts;
    hxhim_t hx;
};

/* holds arguments converted for use by the dataset_* callbacks */
struct dataset_info_t {
    struct file_info_t * file;
    /* subject is just the full path of the dataset */
    char * subject;
    size_t subject_len;
    // hid_t lcpl_id;
    hid_t type_id;
    hid_t space_id;
    // hid_t dcpl_id;
    // hid_t dapl_id;
    // hid_t dxpl_id;
};

struct float3_t {
    float subject;
    float predicate;
    float object;
};

struct double3_t {
    double subject;
    double predicate;
    double object;
};

#endif
