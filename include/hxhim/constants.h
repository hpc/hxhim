#ifndef HXHIM_CONSTANTS_H
#define HXHIM_CONSTANTS_H

#ifdef __cplusplus
#include <cstddef>

extern "C"
{
#else
#include <stddef.h>
#endif

#include "utils/macros.h"

/** Success constant */
#define HXHIM_SUCCESS 3

/** Error constant */
#define HXHIM_ERROR 4

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
 * HXHIM_GETOP_*
 */
#define HXHIM_GETOP_GEN(PREFIX, GEN)                                \
    GEN(PREFIX, EQ)      /** num_recs is ignored */                 \
    GEN(PREFIX, NEXT)                                               \
    GEN(PREFIX, PREV)                                               \
    GEN(PREFIX, FIRST)   /** subject-predicate pair is ignored */   \
    GEN(PREFIX, LAST)    /** subject-predicate pair is ignored */   \
    /* GEN(PREFIX, PRIMARY_EQ) */                                   \
    GEN(PREFIX, INVALID)                                            \

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
