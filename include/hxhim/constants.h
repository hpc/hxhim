#ifndef HXHIM_CONSTANTS_H
#define HXHIM_CONSTANTS_H

#ifdef __cplusplus
extern "C"
{
#endif

/** Success constant */
#define HXHIM_SUCCESS 0

/** Error constant */
#define HXHIM_ERROR 1

#define HXHIM_MAX_BULK_OPS 1000

/** How many different ways a SPO triple will be PUT into mdhim */
#define HXHIM_PUT_MULTIPLER 1

/** The maxium number of operations that can be PUT into MDHIM at once */
#define HXHIM_MAX_BULK_PUT_OPS (HXHIM_MAX_BULK_OPS / HXHIM_PUT_MULTIPLER)

/** The maxium number of operations that can be GET or DEL from MDHIM at once */
#define HXHIM_MAX_BULK_GET_OPS HXHIM_MAX_BULK_OPS
#define HXHIM_MAX_BULK_DEL_OPS HXHIM_MAX_BULK_OPS

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

/**
 * GetOp
 * List of operations that can be done
 * with a Get or BGet
 */
enum hxhim_get_op {
    HXHIM_GET_EQ         = 0,
    HXHIM_GET_NEXT       = 1,
    HXHIM_GET_PREV       = 2,
    HXHIM_GET_FIRST      = 3,
    HXHIM_GET_LAST       = 4,
    HXHIM_GET_PRIMARY_EQ = 5,

    HXHIM_GET_OP_MAX     = 255
};

#ifdef __cplusplus
}
#endif

#endif
