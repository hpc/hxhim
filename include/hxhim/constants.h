#ifndef HXHIM_CONSTANTS_H
#define HXHIM_CONSTANTS_H

#ifdef __cplusplus
#include <cstddef>

extern "C"
{
#else
#include <stddef.h>
#endif

/**
 * Enum and String generation adapted from
 * https://stackoverflow.com/a/10966395
 * by Terrence M
 */
#define GENERATE_ENUM(prefix, name) prefix##_##name,
#define GENERATE_STR(prefix, name)  #prefix "_" #name,

/* not 0 to force return value checks to use this macro */
#define HXHIM_SUCCESS                 3

/** Generic Error */
#define HXHIM_ERROR                   4

/** hxhim::Open Errors */
#define HXHIM_OPEN_ERROR_GEN(PREFIX, GEN)       \
    GEN(PREFIX, BOOTSTRAP)                      \
    GEN(PREFIX, SET_RUNNING)                    \
    GEN(PREFIX, HASH)                           \
    GEN(PREFIX, DATASTORE)                      \
    GEN(PREFIX, TRANSPORT)                      \
    GEN(PREFIX, QUEUES)                         \
    GEN(PREFIX, ASYNC_PUT)

#define HXHIM_OPEN_ERROR_PREFIX HXHIM_OPEN_ERROR

enum hxhim_open_t {
    HXHIM_OPEN_ERROR_GEN(HXHIM_OPEN_ERROR_PREFIX, GENERATE_ENUM)
};

extern const char *HXHIM_OPEN_ERROR_STR[];

/** Different ways a SPO triple can be PUT into HXHIM per PUT */
typedef size_t hxhim_put_permutation_t;
#define HXHIM_PUT_NONE 0x00U
#define HXHIM_PUT_SPO  0x01U
#define HXHIM_PUT_SOP  0x02U
#define HXHIM_PUT_PSO  0x04U
#define HXHIM_PUT_POS  0x08U
#define HXHIM_PUT_OSP  0x10U
#define HXHIM_PUT_OPS  0x20U
#define HXHIM_PUT_ALL  ( HXHIM_PUT_SPO | HXHIM_PUT_SOP | \
                         HXHIM_PUT_PSO | HXHIM_PUT_POS | \
                         HXHIM_PUT_OSP | HXHIM_PUT_OPS )


/** Convenience array for accessing the permutations */
extern const hxhim_put_permutation_t HXHIM_PUT_PERMUTATIONS[];
extern const size_t HXHIM_PUT_PERMUTATIONS_COUNT;

/**
 * hxhim_op_t
 * List of operations done by HXHIM
 *
 * HXHIM_*
 */
#define HXHIM_OP_GEN(PREFIX, GEN)     \
    GEN(PREFIX, PUT)                  \
    GEN(PREFIX, GET)                  \
    GEN(PREFIX, GETOP)                \
    GEN(PREFIX, DELETE)               \
    GEN(PREFIX, SYNC)                 \
    GEN(PREFIX, HISTOGRAM)            \
    GEN(PREFIX, INVALID)              \

#define HXHIM_OP_PREFIX HXHIM

enum hxhim_op_t {
    HXHIM_OP_GEN(HXHIM_OP_PREFIX, GENERATE_ENUM)
};

extern const char *HXHIM_OP_STR[];

/**
 * GetOp Operations
 * List of operations that can be done
 * with a GetOp or BGetOP
 *
 * The operations that use the subject and predicate do prefix
 * searches on key[0, subject len + predicate len)
 *
 * NEXT and LOWEST do the same thing
 *     - find the first value with the prefix and iterate forwards
 *
 * PREV and HIGHEST are different
 *     - PREV finds the first key with the prefix and
 *       iterate backwards
 *     - HIGHEST finds the first key with the prefix
 *       after the given one and iterates backwards
 *
 * HXHIM_GETOP_*
 */
#define HXHIM_GETOP_GEN(PREFIX, GEN)                                      \
    GEN(PREFIX, EQ)      /** num_recs is ignored */                       \
    GEN(PREFIX, NEXT)    /** iterate forwards  */                         \
    GEN(PREFIX, PREV)    /** iterate backwards */                         \
    GEN(PREFIX, FIRST)   /** subject-predicate pair is ignored */         \
    GEN(PREFIX, LAST)    /** subject-predicate pair is ignored */         \
    GEN(PREFIX, LOWEST)  /** iterate forwards */                          \
    GEN(PREFIX, HIGHEST) /** go to end of prefix and iterate backwards */ \
    /* GEN(PREFIX, PRIMARY_EQ) */                                         \
    GEN(PREFIX, INVALID)                                                  \

#define HXHIM_GETOP_PREFIX HXHIM_GETOP

enum hxhim_getop_t {
    HXHIM_GETOP_GEN(HXHIM_GETOP_PREFIX, GENERATE_ENUM)
};

extern const char *HXHIM_GETOP_STR[];

/**
 * hxhim_data_t
 * The types of data tha can be passed into HXHIM
 *
 * HXHIM_DATA_*
 */
#define HXHIM_DATA_GEN(PREFIX, GEN) \
    GEN(PREFIX, INVALID)            \
    GEN(PREFIX, INT32)              \
    GEN(PREFIX, INT64)              \
    GEN(PREFIX, UINT32)             \
    GEN(PREFIX, UINT64)             \
    GEN(PREFIX, FLOAT)              \
    GEN(PREFIX, DOUBLE)             \
    GEN(PREFIX, BYTE)               \
    GEN(PREFIX, POINTER)            \
    GEN(PREFIX, MAX_KNOWN)          \

#define HXHIM_DATA_PREFIX HXHIM_DATA

enum hxhim_data_t {
    HXHIM_DATA_GEN(HXHIM_DATA_PREFIX, GENERATE_ENUM)
};

extern const char *HXHIM_DATA_STR[];

#ifdef __cplusplus
}
#endif

#endif
