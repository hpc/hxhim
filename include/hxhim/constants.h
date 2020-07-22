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
 * Adapted from
 * https://stackoverflow.com/a/10966395
 * by Terrence M
 */
#define HXHIM_OP_GEN(GEN)     \
    GEN(PUT)                  \
    GEN(GET)                  \
    GEN(GETOP)                \
    GEN(DELETE)               \
    GEN(SYNC)                 \
    GEN(INVALID)              \

#define GENERATE_ENUM(name) HXHIM_##name,
#define GENERATE_STR(name) "HXHIM_" #name,

/**
 * hxhim_op_t
 * List of operations done by HXHIM
 */
enum hxhim_op_t {
    HXHIM_OP_GEN(GENERATE_ENUM)
};

/**
 * HXHIM_OP_STR
 * Get string versions of hxhim_op_t
 */
extern const char *HXHIM_OP_STR[];

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

/**
 * hxhim_object_type_t
 * The types of data tha can be passed into HXHIM
 */
enum hxhim_object_type_t {
    HXHIM_OBJECT_TYPE_INT = 0,
    HXHIM_OBJECT_TYPE_SIZE,
    HXHIM_OBJECT_TYPE_INT64,
    HXHIM_OBJECT_TYPE_FLOAT,
    HXHIM_OBJECT_TYPE_DOUBLE,
    HXHIM_OBJECT_TYPE_BYTE,

    HXHIM_OBJECT_TYPE_INVALID
};

#ifdef __cplusplus
}
#endif

#endif
