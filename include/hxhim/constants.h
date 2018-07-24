#ifndef HXHIM_CONSTANTS_H
#define HXHIM_CONSTANTS_H

#include "mdhim/constants.h"
#include "get_op.h"

#ifdef __cplusplus
extern "C"
{
#endif

/** Success constant */
#define HXHIM_SUCCESS 0

/** Error constant */
#define HXHIM_ERROR 1

#define HXHIM_MAX_BULK_OPS MAX_BULK_OPS

/** How many different ways a SPO triple will be PUT into mdhim */
#define HXHIM_PUT_MULTIPLER 1

/** The maxium number of operations that can be PUT into MDHIM at once */
#define HXHIM_MAX_BULK_PUT_OPS (MAX_BULK_OPS / HXHIM_PUT_MULTIPLER)

/** The maxium number of operations that can be GET or DEL from MDHIM at once */
#define HXHIM_MAX_BULK_GET_OPS HXHIM_MAX_BULK_OPS
#define HXHIM_MAX_BULK_DEL_OPS HXHIM_MAX_BULK_OPS

/** HXHIM Option Values */
#define HXHIM_OPT_COMM_NULL     (1 << 0)
#define HXHIM_OPT_COMM_MPI      (1 << 1)
#define HXHIM_OPT_COMM_THALLIUM (1 << 2)
#define HXHIM_OPT_STORE_NULL    (1 << 8)
#define HXHIM_OPT_STORE_LEVELDB (1 << 9)

typedef enum hxhim_backend {
    HXHIM_BACKEND_MDHIM,
    HXHIM_BACKEND_LEVELDB,
    HXHIM_BACKEND_IN_MEMORY,
} hxhim_backend_t;

typedef enum hxhim_spo_type {
    HXHIM_SPO_INT_TYPE,
    HXHIM_SPO_SIZE_TYPE,
    HXHIM_SPO_INT64_TYPE,
    HXHIM_SPO_FLOAT_TYPE,
    HXHIM_SPO_DOUBLE_TYPE,
    HXHIM_SPO_BYTE_TYPE,
} hxhim_spo_type_t;

#ifdef __cplusplus
}
#endif

#endif
