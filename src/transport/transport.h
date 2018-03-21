#ifndef HXHIM_TRANSPORT_TYPES
#define HXHIM_TRANSPORT_TYPES

#include "mdhim_constants.h"
#include "transport_constants.h"

#ifdef __cplusplus
extern "C"
{
#endif

/*
 * Forward declarations of opaque types that
 * actually implement the data structures
*/
typedef struct mdhim_basem_private  mdhim_basem_private_t;
typedef struct mdhim_putm_private   mdhim_putm_private_t;
typedef struct mdhim_bputm_private  mdhim_bputm_private_t;
typedef struct mdhim_getm_private   mdhim_getm_private_t;
typedef struct mdhim_bgetm_private  mdhim_bgetm_private_t;
typedef struct mdhim_delm_private   mdhim_delm_private_t;
typedef struct mdhim_bdelm_private  mdhim_bdelm_private_t;
typedef struct mdhim_rm_private     mdhim_rm_private_t;
typedef struct mdhim_getrm_private  mdhim_getrm_private_t;
typedef struct mdhim_bgetrm_private mdhim_bgetrm_private_t;
typedef struct mdhim_brm_private    mdhim_brm_private_t;

/* Put message */
typedef struct mdhim_putm {
    mdhim_putm_private_t *p;
} mdhim_putm_t;

/* Bulk put message */
typedef struct mdhim_bputm {
    mdhim_bputm_private_t *p;
} mdhim_bputm_t;

/* Get record message */
typedef struct mdhim_getm {
    mdhim_getm_private_t *p;
} mdhim_getm_t;

/* Bulk get record message */
typedef struct mdhim_bgetm {
    mdhim_bgetm_private_t *p;
} mdhim_bgetm_t;

/* Delete message */
typedef struct mdhim_delm {
    mdhim_delm_private_t *p;
} mdhim_delm_t;

/* Bulk delete record message */
typedef struct mdhim_bdelm {
    mdhim_bdelm_private_t *p;
} mdhim_bdelm_t;

/* Generic receive message */
typedef struct mdhim_rm {
    mdhim_rm_private_t *p;
} mdhim_rm_t;

/* get receive message */
typedef struct mdhim_getrm {
    mdhim_getrm_private_t *p;
} mdhim_getrm_t;

/* Bulk get receive message */
typedef struct mdhim_bgetrm {
    mdhim_bgetrm_private_t *p;
} mdhim_bgetrm_t;

/* Bulk generic receive message */
typedef struct mdhim_brm {
    mdhim_brm_private_t *p;
} mdhim_brm_t;

/**
 * Destruction functions for the mdhim structures
 *
 * Users do never create mdhim structures, so
 * they do not have access to the initialization
 * functions.
*/
void mdhim_pm_destroy(mdhim_putm_t *pm);
void mdhim_bpm_destroy(mdhim_bputm_t *bpm);
void mdhim_gm_destroy(mdhim_getm_t *gm);
void mdhim_bgm_destroy(mdhim_bgetm_t *bgm);
void mdhim_delm_destroy(mdhim_delm_t *dm);
void mdhim_bdelm_destroy(mdhim_bdelm_t *bdm);
void mdhim_rm_destroy(mdhim_rm_t *rm);
void mdhim_grm_destroy(mdhim_getrm_t *bgrm);
void mdhim_bgrm_destroy(mdhim_bgetrm_t *bgrm);
void mdhim_brm_destroy(mdhim_brm_t *brm);

/**
 * Accessor functions for the structs
 *
 * These will need to be organized later on.
*/
int mdhim_brm_error(const mdhim_brm_t *brm, int *error);

int mdhim_grm_error(const mdhim_getrm_t *grm, int *error);
int mdhim_grm_key(const mdhim_getrm_t *grm, void **key, int *key_len);
int mdhim_grm_value(const mdhim_getrm_t *grm, void **value, int *value_len);

int mdhim_bgrm_error(const mdhim_bgetrm_t *bgrm, int *error);
int mdhim_bgrm_keys(const mdhim_bgetrm_t *bgrm, void ***keys, int **key_lens);
int mdhim_bgrm_values(const mdhim_bgetrm_t *bgrm, void ***values, int **value_lens);
int mdhim_bgrm_num_keys(const mdhim_bgetrm_t *bgrm, int *num_keys);


#ifdef __cplusplus
}
#endif

#endif
