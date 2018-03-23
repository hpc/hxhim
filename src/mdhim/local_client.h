/*
 * MDHIM TNG
 *
 * Local client implementation
 */

#ifndef      __LOCAL_CLIENT_H
#define      __LOCAL_CLIENT_H

#include "transport.hpp"

TransportRecvMessage *local_client_put(mdhim_t *md, TransportPutMessage *pm);
TransportGetRecvMessage *local_client_get(mdhim_t *md, TransportGetMessage *gm);
// TransportRecvMessage *local_client_bput(mdhim_t *md, TransportBPutMessage *bpm);
// TransportBGetRecvMessage *local_client_bget(mdhim_t *md, TransportBGetMessage *bgm);
// TransportBGetRecvMessage *local_client_bget_op(mdhim_t *md, TransportGetMessage *gm);

TransportRecvMessage *local_client_commit(mdhim_t *md, TransportMessage *cm);

// mdhim_t_rm_t *local_client_delete(mdhim_t *md, mdhim_t_delm_t *dm);
// mdhim_t_rm_t *local_client_bdelete(mdhim_t *md, mdhim_t_bdelm_t *dm);
// void local_client_close(mdhim_t *md, mdhim_t_basem_t *cm);

#endif
