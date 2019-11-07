#ifndef HXHIM_HXF5_VOL_STRUCTS_H
#define HXHIM_HXF5_VOL_STRUCTS_H

#include <stdio.h>

#include "hxhim_vol.h"

/* holds arguments converted for use by the file_* callbacks */
struct file_info_t {
    FILE * file;
    hxhim_options_t opts;
    hxhim_t hx;
    hid_t id;
    void * info;
};

/* holds arguments converted for use by the dataset_* callbacks */
struct dataset_info_t {
    hxhim_t * hx;
    hid_t type_id;
    hid_t space_id;
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
