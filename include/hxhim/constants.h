#ifndef HXHIM_CONSTANTS_H
#define HXHIM_CONSTANTS_H

#ifdef __cplusplus
#include <cstddef>

extern "C"
{
#else
#include <stddef.h>
#endif

/** Success constant */
#define HXHIM_SUCCESS 3

/** Error constant */
#define HXHIM_ERROR 4

/**
 * Enum and String generation adapted from
 * https://stackoverflow.com/a/10966395
 * by Terrence M
 */
#define GENERATE_ENUM(prefix, name) prefix##_##name,
#define GENERATE_STR(prefix, name)  #prefix "-" #name,

/** Different ways a SPO triple will be PUT into HXHIM per PUT */
#define HXHIM_PUT_PERMUTATION_GEN(PREFIX, GEN)  \
    GEN(PREFIX, SPO) /* always enabled */       \
    GEN(PREFIX, SOP)                            \
    GEN(PREFIX, PSO)                            \
    GEN(PREFIX, POS)                            \
    GEN(PREFIX, OSP)                            \
    GEN(PREFIX, OPS)                            \

#define HXHIM_PUT_PERMUTATION_PREFIX HXHIM_PUT_PERMUTATION

typedef enum {
    HXHIM_PUT_PERMUTATION_GEN(HXHIM_PUT_PERMUTATION_PREFIX, GENERATE_ENUM)
} HXHIM_PUT_PERMUTATION;

extern const char *HXHIM_PUT_PERMUTATION_STR[];

/** Enabled permutations in array form */
extern const HXHIM_PUT_PERMUTATION HXHIM_PUT_PERMUTATIONS_ENABLED[];

/** Function to check if a permutation has been enabled */
int hxhim_put_permutation_enabled(const HXHIM_PUT_PERMUTATION combo);

/** Number of permutations enabled */
extern const size_t HXHIM_PUT_MULTIPLIER;

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
#define HXHIM_GETOP_GEN(PREFIX, GEN)                                    \
    GEN(PREFIX, EQ)      /** num_recs is ignored */                     \
    GEN(PREFIX, NEXT)                                                   \
    GEN(PREFIX, PREV)                                                   \
    GEN(PREFIX, FIRST)   /** subject-predicate pair is ignored */       \
    GEN(PREFIX, LAST)    /** subject-predicate pair is ignored */       \
    /* GEN(PREFIX, PRIMARY_EQ) */                                       \
    GEN(PREFIX, INVALID)                                                \

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
    GEN(PREFIX, INT)                \
    GEN(PREFIX, SIZE)               \
    GEN(PREFIX, INT64)              \
    GEN(PREFIX, UINT64)             \
    GEN(PREFIX, FLOAT)              \
    GEN(PREFIX, DOUBLE)             \
    GEN(PREFIX, BYTE)               \
    GEN(PREFIX, POINTER)            \

#define HXHIM_DATA_PREFIX HXHIM_DATA

enum hxhim_data_t {
    HXHIM_DATA_GEN(HXHIM_DATA_PREFIX, GENERATE_ENUM)
};

extern const char *HXHIM_DATA_STR[];

#ifdef __cplusplus
}
#endif

#endif
