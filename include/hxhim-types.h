#ifndef HXHIM_TYPES_H
#define HXHIM_TYPES_H

#include "mdhim_constants.h"

#ifdef __cplusplus
extern "C"
{
#endif

/** Success constant */
#define HXHIM_SUCCESS 0

/** Error constant */
#define HXHIM_ERROR 1

/** The maxium number of operations that can be PUT into MDHIM at once */
#define HXHIM_MAX_BULK_PUT_OPS (MAX_BULK_OPS / 6)

/** The maxium number of operations that can be GET or DEL from MDHIM at once */
#define HXHIM_MAX_BULK_GET_OPS MAX_BULK_OPS
#define HXHIM_MAX_BULK_DEL_OPS MAX_BULK_OPS

/** HXHIM Option Values */
#define HXHIM_OPT_COMM_NULL     (1 << 0)
#define HXHIM_OPT_COMM_MPI      (1 << 1)
#define HXHIM_OPT_COMM_THALLIUM (1 << 2)
#define HXHIM_OPT_STORE_NULL    (1 << 8)
#define HXHIM_OPT_STORE_LEVELDB (1 << 9)

typedef struct hxhim_private hxhim_private_t;

/**
 * HXHIM session data
 */
typedef struct {
    hxhim_private_t *p;
} hxhim_t;

#ifdef __cplusplus
}
#endif // __cplusplus

#endif
