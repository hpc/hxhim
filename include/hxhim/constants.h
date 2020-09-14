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
#define HXHIM_SUCCESS 0

/** Error constant */
#define HXHIM_ERROR 1

/**
 * Enum and String generation adapted from
 * https://stackoverflow.com/a/10966395
 * by Terrence M
 */
#define GENERATE_ENUM(prefix, name) prefix##_##name,
#define GENERATE_STR(prefix, name)  #prefix "-" #name,

/** Different ways a SPO triple will be PUT into HXHIM per PUT */
#define HXHIM_PUT_COMBINATION_GEN(PREFIX, GEN)  \
    GEN(PREFIX, SPO) /* always enabled */       \
    GEN(PREFIX, SOP)                            \
    GEN(PREFIX, PSO)                            \
    GEN(PREFIX, POS)                            \
    GEN(PREFIX, OSP)                            \
    GEN(PREFIX, OPS)                            \

#define HXHIM_PUT_COMBINATION_PREFIX HXHIM_PUT_COMBINATION

typedef enum {
    HXHIM_PUT_COMBINATION_GEN(HXHIM_PUT_COMBINATION_PREFIX, GENERATE_ENUM)
} HXHIM_PUT_COMBINATION;

extern const char *HXHIM_PUT_COMBINATION_STR[];

/** Enabled combinations in array form */
extern const HXHIM_PUT_COMBINATION HXHIM_PUT_COMBINATIONS_ENABLED[];

/** Function to check if a combination has been enabled */
int hxhim_put_combination_enabled(const HXHIM_PUT_COMBINATION combo);

/** Number of combinations enabled */
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
 * hxhim_object_type_t
 * The types of data tha can be passed into HXHIM
 *
 * HXHIM_OBJECT_TYPE_*
 */
#define HXHIM_OBJECT_TYPE_GEN(PREFIX, GEN) \
    GEN(PREFIX, INT)                       \
    GEN(PREFIX, SIZE)                      \
    GEN(PREFIX, INT64)                     \
    GEN(PREFIX, FLOAT)                     \
    GEN(PREFIX, DOUBLE)                    \
    GEN(PREFIX, BYTE)                      \
    GEN(PREFIX, INVALID)                   \

#define HXHIM_OBJECT_TYPE_PREFIX HXHIM_OBJECT_TYPE

enum hxhim_object_type_t {
    HXHIM_OBJECT_TYPE_GEN(HXHIM_OBJECT_TYPE_PREFIX, GENERATE_ENUM)
};

extern const char *HXHIM_OBJECT_TYPE_STR[];

#ifdef __cplusplus
}
#endif

#endif
