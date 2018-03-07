#ifndef      __MESSAGES_H
#define      __MESSAGES_H

#include "range_server.h"
#include "mdhim.h"
#include "transport.h"

#ifdef __cplusplus
extern "C"
{
#endif

int send_client_response(struct mdhim *md, int dest, TransportMessage *message, int *sizebuf,
                         void **sendbuf, MPI_Request **size_req, MPI_Request **msg_req);

int receive_rangesrv_work(struct mdhim *md, int *src, TransportMessage **message);

#ifdef __cplusplus
}
#endif
#endif
