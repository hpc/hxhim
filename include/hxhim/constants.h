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

/** How many different ways a SPO triple will be PUT into HXHIM per PUT */
#define HXHIM_PUT_MULTIPLIER 1

/**
 * hxhim_type_t
 * The types of data tha can be passed into HXHIM
 */
enum hxhim_type_t {
    HXHIM_INT_TYPE = 0,
    HXHIM_SIZE_TYPE,
    HXHIM_INT64_TYPE,
    HXHIM_FLOAT_TYPE,
    HXHIM_DOUBLE_TYPE,
    HXHIM_BYTE_TYPE,

    HXHIM_INVALID_TYPE
};

/**
 * GetOp
 * List of operations that can be done
 * with a GetOp or BGetOP
 */
enum hxhim_get_op_t {
    HXHIM_GET_EQ = 0,      /** num_recs is ignored */
    HXHIM_GET_NEXT,
    HXHIM_GET_PREV,

    HXHIM_GET_FIRST,       /** subject-predicate pair is ignored */
    HXHIM_GET_LAST,        /** subject-predicate pair is ignored */

    // HXHIM_GET_PRIMARY_EQ,

    HXHIM_GET_INVALID
};

#ifdef __cplusplus
}
#endif

#endif
