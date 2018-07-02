#ifndef MDHIM_TRANSPORT_MPI_RANGE_SERVER_HPP
#define MDHIM_TRANSPORT_MPI_RANGE_SERVER_HPP

#include <pthread.h>
#include <unistd.h>

#include <mpi.h>

#include "MPIPacker.hpp"
#include "MPIUnpacker.hpp"
#include "mdhim/mdhim_struct.h"
#include "mdhim/private.h"
#include "mdhim/work_item.h"
#include "transport.hpp"
#include "utils/MemoryManagers.hpp"

class MPIRangeServer {
    public:
        static int init(mdhim_t *md, FixedBufferPool *fbp, const std::size_t listener_count);
        static void destroy();

        /**
         * Function that will be used to listen for MPI messages
         */
        static void *listener_thread(void *data);

        static void Flush(MPI_Request *req, int *flag, MPI_Status *status, volatile int &shutdown);

        static int only_send_client_response(int dest, void *sendbuf, std::size_t sizebuf, volatile int &shutdown);
        static int send_client_response(work_item_t *item, TransportMessage *message, volatile int &shutdown);

        static int only_receive_rangesrv_work(void **recvbuf, std::size_t *recvsize, volatile int &shutdown);
        static int receive_rangesrv_work(TransportMessage **message, volatile int &shutdown);

        static std::size_t listener_count_;
        static pthread_t *listeners_;
        static pthread_mutex_t mutex_;
        static FixedBufferPool *fbp_;
};

#endif
