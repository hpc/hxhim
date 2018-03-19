#ifndef HXHIM_TRANSPORT_MPI_ENDPOINT_BASE
#define HXHIM_TRANSPORT_MPI_ENDPOINT_BASE

#include <pthread.h>
#include <stdexcept>
#include <unistd.h>

#include "mlog2.h"
#include "mlogfacs2.h"
#include <mpi.h>

#include "mdhim_constants.h"
#include "transport.hpp"

/**
 * MPIEndpointBase
 * Base class containing common values and functions for MPI Transport related classes
 */
class MPIEndpointBase {
    public:
        MPIEndpointBase(const MPI_Comm comm, volatile int &shutdown);
        virtual ~MPIEndpointBase();

        MPI_Comm Comm() const;
        int Rank() const;
        int Size() const;

        /**
         * Functions called by the client
         */
        int send_rangesrv_work(int dest, const void *buf, const int size);
        int send_all_rangesrv_work(void **messages, int *sizes, int *dests, int num_srvs);
        int receive_client_response(int src, void **recvbuf, int *recvsize);
        int receive_all_client_responses(int *srcs, int nsrcs, void ***recvbufs, int **sizebuf);

        /**
         * Functions called by the range server
         */
        static int respond_to_client(Transport *transport, const TransportAddress *dest, TransportMessage *message);
        static int listen_for_client(Transport *transport, TransportAddress **src, TransportMessage **message);

        static void Flush(MPIEndpointBase *epb, MPI_Request *req, int *flag, MPI_Status *status);
        void Flush(MPI_Request *req);

    protected:
        MPI_Comm comm_;
        static pthread_mutex_t mutex_;

        int rank_;
        int size_;

        volatile int &shutdown_;

    private:
        static int only_send_client_response(MPIEndpointBase *epb, int dest, void *sendbuf, int sizebuf);
        static int send_client_response(MPIEndpointBase *epb, int dest, TransportMessage *message);
        static int only_receive_rangesrv_work(MPIEndpointBase *epb, int *src, void **recvbuf, int *recvsize);
        static int receive_rangesrv_work(MPIEndpointBase *epb, int *src, TransportMessage **message);

};

#endif
