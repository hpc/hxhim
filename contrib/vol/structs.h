#ifndef HXHIM_HXF5_VOL_STRUCTS_H
#define HXHIM_HXF5_VOL_STRUCTS_H

#include <pthread.h>
#include <stdio.h>

#include "hxhim_vol.h"

/* holds external values to be made accessible to the VOL */
struct under_info_t {
    hid_t id;

    int comm_dup;
    MPI_Comm comm;
    hxhim_options_t opts;
    hxhim_t hx;
};

/* holds arguments converted for use by the file_* callbacks */
struct file_info_t {
    struct under_info_t * under_vol;

    char * name;
    unsigned flags;
    hid_t fapl_id;
};

/* holds arguments converted for use by the dataset_* callbacks */
struct dataset_info_t {
    struct file_info_t * file;
    /* subject is just the full path of the dataset */
    char * subject;
    size_t subject_len;
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
