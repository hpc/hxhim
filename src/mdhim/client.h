/*
 * MDHIM TNG
 *
 * Client specific implementation
 */

#ifndef      __CLIENT_H
#define      __CLIENT_H

#include "indexes.h"
#include "transport.h"

TransportRecvMessage *client_put(mdhim_t *md, TransportPutMessage *pm);
TransportGetRecvMessage *client_get(mdhim_t *md, TransportGetMessage *gm);

// TransportBRecvMessage *client_bput(mdhim_t *md, index_t *index, TransportBPutMessage **bpm_list);
// TransportBGetRecvMessage *client_bget(mdhim_t *md, index_t *index, TransportBGetMessage **bgm_list);

// mdhim_t_bgetrm_t *client_bget_op(mdhim_t *md, mdhim_t_getm_t *gm);
// mdhim_t_rm_t *client_delete(mdhim_t *md, mdhim_t_delm_t *dm);
// mdhim_t_brm_t *client_bdelete(mdhim_t *md, index_t *index,
//                                    mdhim_t_bdelm_t **bdm_list);

#endif
