#ifndef HXHIM_TRANSPORT_MPI_TRANSPORT_BASE
#define HXHIM_TRANSPORT_MPI_TRANSPORT_BASE

#include <pthread.h>
#include <stdexcept>
#include <unistd.h>

#include "mlog2.h"
#include "mlogfacs2.h"
#include <mpi.h>

#include "mdhim_constants.h"

/**
 * MPITransportBase
 * Base class containing common values and functions for MPI Transport related classes
 */
class MPITransportBase {
    public:
    MPITransportBase(const MPI_Comm comm, volatile int &shutdown);
        virtual ~MPITransportBase();

        MPI_Comm Comm() const;
        int Rank() const;
        int Size() const;

        /** functions that actually call MPI */
        int send_rangesrv_work(int dest, const void *buf, const int size);
        int send_all_rangesrv_work(void **messages, int *sizes, int *dests, int num_srvs);
        int send_client_response(int dest, int *sizebuf, void **sendbuf, MPI_Request **size_req, MPI_Request **msg_req);
        int receive_client_response(int src, void **recvbuf, int *recvsize);
        int receive_all_client_responses(int *srcs, int nsrcs, void ***recvbufs, int **sizebuf);

        void Flush(MPI_Request *req);

    protected:
        MPI_Comm comm_;
        static pthread_mutex_t mutex_;

        int rank_;
        int size_;

        volatile int &shutdown_;
};

#endif
