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

#define HXHIM_MAX_BULK_OPS 500

/** How many different ways a SPO triple will be PUT into mdhim */
#define HXHIM_PUT_MULTIPLER 1

/** The maxium number of operations that can be PUT into MDHIM at once */
#define HXHIM_MAX_BULK_PUT_OPS (HXHIM_MAX_BULK_OPS / HXHIM_PUT_MULTIPLER)

/** The maxium number of operations that can be GET or DEL from MDHIM at once */
#define HXHIM_MAX_BULK_GET_OPS HXHIM_MAX_BULK_OPS
#define HXHIM_MAX_BULK_DEL_OPS HXHIM_MAX_BULK_OPS

/**
 * hxhim_datastore_t
 * The types of datastores that are available
 */
enum hxhim_datastore_t {
    HXHIM_DATASTORE_LEVELDB,
    HXHIM_DATASTORE_IN_MEMORY,
};

/**
 * hxhim_type_t
 * The types of data tha can be passed into HXHIM
 */
enum hxhim_type_t {
    HXHIM_INT_TYPE,
    HXHIM_SIZE_TYPE,
    HXHIM_INT64_TYPE,
    HXHIM_FLOAT_TYPE,
    HXHIM_DOUBLE_TYPE,
    HXHIM_BYTE_TYPE,
};

/**
 * GetOp
 * List of operations that can be done
 * with a Get or BGet
 */
enum hxhim_get_op_t {
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
