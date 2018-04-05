#ifndef HXHIM_TRANSPORT_PRIVATE_TYPES
#define HXHIM_TRANSPORT_PRIVATE_TYPES

#include "transport.h"
#include "transport.hpp"

/**
 * Implementation of private transport structs
*/

/* Put message */
typedef struct mdhim_putm_private {
    TransportPutMessage *pm;
} mdhim_putm_private_t;

/* Bulk put message */
typedef struct mdhim_bputm_private {
    TransportBPutMessage *bpm;
} mdhim_bputm_private_t;

/* Get record message */
typedef struct mdhim_getm_private {
    TransportGetMessage *gm;
} mdhim_getm_private_t;

/* Bulk get record message */
typedef struct mdhim_bgetm_private {
    TransportBGetMessage *bgm;
} mdhim_bgetm_private_t;

/* Delete message */
typedef struct mdhim_delm_private {
    TransportDeleteMessage *dm;
} mdhim_delm_private_t;

/* Bulk delete record message */
typedef struct mdhim_bdelm_private {
    TransportBDeleteMessage * bdm;
} mdhim_bdelm_private_t;

/* Generic receive message */
typedef struct mdhim_rm_private {
    TransportRecvMessage *rm;
} mdhim_rm_private_t;

/* Bulk get receive message */
typedef struct mdhim_getrm_private {
    TransportGetRecvMessage *grm;
} mdhim_getrm_private_t;

/* Bulk get receive message */
typedef struct mdhim_bgetrm_private {
    TransportBGetRecvMessage *bgrm;
} mdhim_bgetrm_private_t;

/* Bulk generic receive message */
typedef struct mdhim_brm_private {
    TransportBRecvMessage *brm;
} mdhim_brm_private_t;

/**
 * Initialization functions for the mdhim structures
 *
 * These functsios are private because users never
 * allocate the mdhim structures.
*/
mdhim_putm_t   *mdhim_pm_init(TransportPutMessage *pm = nullptr);
mdhim_bputm_t  *mdhim_bpm_init(TransportBPutMessage *bpm = nullptr);
mdhim_getm_t   *mdhim_gm_init(TransportGetMessage *gm = nullptr);
mdhim_bgetm_t  *mdhim_bgm_init(TransportBGetMessage *bgm = nullptr);
mdhim_delm_t   *mdhim_delm_init(TransportDeleteMessage *dm = nullptr);
mdhim_bdelm_t  *mdhim_bdelm_init(TransportBDeleteMessage *bdm = nullptr);
mdhim_rm_t     *mdhim_rm_init(TransportRecvMessage *rm = nullptr);
mdhim_getrm_t  *mdhim_grm_init(TransportGetRecvMessage *grm = nullptr);
mdhim_bgetrm_t *mdhim_bgrm_init(TransportBGetRecvMessage *bgrm = nullptr);
mdhim_brm_t    *mdhim_brm_init(TransportBRecvMessage *brm = nullptr);

#endif
