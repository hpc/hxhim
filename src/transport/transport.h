#ifndef HXHIM_TRANSPORT_TYPES
#define HXHIM_TRANSPORT_TYPES

#include <stddef.h>

#include "mdhim_constants.h"
#include "transport_constants.h"

#ifdef __cplusplus
extern "C"
{
#endif

/*
 * Forward declarations of opaque types that
 * actually implement the data structures.
 *
 * Only TransportMessages that are returned
 * by operations have structs wrappers.
 *
 * Users should not (and cannot) declare
 * static variables of these types. Every
 * instance will come from mdhim.
*/
typedef struct mdhim_rm     mdhim_rm_t;     // Generic Receive message
typedef struct mdhim_brm    mdhim_brm_t;    // Bulk Generic Receive message
typedef struct mdhim_getrm  mdhim_getrm_t;  // Get Receive message
typedef struct mdhim_bgetrm mdhim_bgetrm_t; // Bulk Get Receive message

/**
 * Destruction functions for the mdhim structures
 *
 * Users never create mdhim structures, so
 * they do not have access to the initialization
 * functions.
*/
void mdhim_rm_destroy(mdhim_rm_t *rm);
void mdhim_brm_destroy(mdhim_brm_t *brm);
void mdhim_grm_destroy(mdhim_getrm_t *grm);
void mdhim_bgrm_destroy(mdhim_bgetrm_t *bgrm);

/**
 * Accessor functions for the structs
 * The pointers used to extract values from the structures
 * should be the addresses of predefined locations that
 * are owned by the caller - No memory is allocated by
 * these functions.
 *
 * These will need to be organized later on.
 *
*/
int mdhim_rm_src(const mdhim_rm_t *rm, int *src);
int mdhim_rm_rs_idx(const mdhim_rm_t *rm, int *rs_idx);
int mdhim_rm_error(const mdhim_rm_t *rm, int *error);

int mdhim_brm_src(const mdhim_brm_t *brm, int *src);
int mdhim_brm_rs_idx(const mdhim_brm_t *brm, int **rs_idx);
int mdhim_brm_error(const mdhim_brm_t *brm, int *error);
int mdhim_brm_num_keys(const mdhim_brm_t *brm, size_t *num_keys);
int mdhim_brm_next(const mdhim_brm_t *brm, mdhim_brm_t **next);

int mdhim_grm_src(const mdhim_getrm_t *grm, int *src);
int mdhim_grm_rs_idx(const mdhim_getrm_t *grm, int *rs_idx);
int mdhim_grm_error(const mdhim_getrm_t *grm, int *error);
int mdhim_grm_key(const mdhim_getrm_t *grm, void **key, size_t *key_len);
int mdhim_grm_value(const mdhim_getrm_t *grm, void **value, size_t *value_len);

int mdhim_bgrm_src(const mdhim_bgetrm_t *bgrm, int *src);
int mdhim_bgrm_rs_idx(const mdhim_bgetrm_t *bgrm, int **rs_idx);
int mdhim_bgrm_error(const mdhim_bgetrm_t *bgrm, int *error);
int mdhim_bgrm_num_keys(const mdhim_bgetrm_t *bgrm, size_t *num_keys);
int mdhim_bgrm_keys(const mdhim_bgetrm_t *bgrm, void ***keys, size_t **key_lens);
int mdhim_bgrm_values(const mdhim_bgetrm_t *bgrm, void ***values, size_t **value_lens);
int mdhim_bgrm_next(const mdhim_bgetrm_t *bgrm, mdhim_bgetrm_t **next);

#ifdef __cplusplus
}
#endif

#endif
