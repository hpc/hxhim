#ifndef HXHIM_TRANSPORT_MPI_RANGE_SERVER
#define HXHIM_TRANSPORT_MPI_RANGE_SERVER

#include <pthread.h>
#include <unistd.h>

#include <mpi.h>

#include "mdhim_private.h"
#include "transport.hpp"
#include "work_item.h"
#include "MemoryManagers.hpp"
#include "MPIPacker.hpp"
#include "MPIUnpacker.hpp"

class MPIRangeServer {
    public:
        /**
         * Function that will be used to listen for MPI messages
         */
        static void *listener_thread(void *data);

        static void Flush(MPI_Request *req, int *flag, MPI_Status *status, volatile int &shutdown);

        static int only_send_client_response(int dest, void *sendbuf, int sizebuf, volatile int &shutdown);
        static int send_client_response(work_item_t *item, TransportMessage *message, volatile int &shutdown);

        static int only_receive_rangesrv_work(void **recvbuf, int *recvsize, volatile int &shutdown);
        static int receive_rangesrv_work(TransportMessage **message, volatile int &shutdown);

        static pthread_mutex_t mutex_;
};

#endif
