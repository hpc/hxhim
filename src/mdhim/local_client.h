/*
 * MDHIM TNG
 *
 * Local client implementation
 */

#ifndef      __LOCAL_CLIENT_H
#define      __LOCAL_CLIENT_H

#include "mdhim_struct.h"
#include "transport.hpp"

TransportRecvMessage *local_client_put(mdhim_t *md, TransportPutMessage *pm);
TransportGetRecvMessage *local_client_get(mdhim_t *md, TransportGetMessage *gm);
TransportBRecvMessage *local_client_bput(mdhim_t *md, TransportBPutMessage *bpm);
TransportBGetRecvMessage *local_client_bget(mdhim_t *md, TransportBGetMessage *bgm);
TransportBGetRecvMessage *local_client_bget_op(mdhim_t *md, TransportGetMessage *gm);
TransportBRecvMessage *local_client_commit(mdhim_t *md, TransportMessage *cm);
TransportRecvMessage *local_client_delete(mdhim_t *md, TransportDeleteMessage *dm);
TransportBRecvMessage *local_client_bdelete(mdhim_t *md, TransportBDeleteMessage *bdm);
// void local_client_close(mdhim_t *md, TransportMessage *cm);

#endif
