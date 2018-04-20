#ifndef HXHIM_TRANSPORT_PRIVATE_TYPES
#define HXHIM_TRANSPORT_PRIVATE_TYPES

#include "transport.h"
#include "transport.hpp"

/**
 * TransportMessage struct wrappers
*/

/* Bulk Generic Receive message */
typedef struct mdhim_brm {
    TransportBRecvMessage *brm;
} mdhim_brm_t;

/* Get Receive message */
typedef struct mdhim_getrm {
    TransportGetRecvMessage *grm;
} mdhim_getrm_t;

/* Bulk Get Receive message */
typedef struct mdhim_bgetrm {
    TransportBGetRecvMessage *bgrm;
} mdhim_bgetrm_t;

/**
 * Initialization functions for the mdhim structures
 *
 * These functsios are private because users never
 * allocate the mdhim structures.
*/
mdhim_getrm_t  *mdhim_grm_init(TransportGetRecvMessage *grm = nullptr);
mdhim_bgetrm_t *mdhim_bgrm_init(TransportBGetRecvMessage *bgrm = nullptr);
mdhim_brm_t    *mdhim_brm_init(TransportBRecvMessage *brm = nullptr);

#endif
