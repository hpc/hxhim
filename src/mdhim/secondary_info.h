#ifndef MDHIM_SECONDARY_INFO_STRUCT
#define MDHIM_SECONDARY_INFO_STRUCT

#include "indexes.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct secondary_info {
    struct index *secondary_index;
    void **secondary_keys;
    int *secondary_key_lens;
    int num_keys;
    int info_type;
} secondary_info_t;

typedef struct secondary_bulk_info {
    struct index *secondary_index;
    void ***secondary_keys;
    int **secondary_key_lens;
    int *num_keys;
    int num_records;
    int info_type;
} secondary_bulk_info_t;

#ifdef __cplusplus
}
#endif
#endif
