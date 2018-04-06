#ifndef HXHIM_TRANSPORT_MPI_PACKER
#define HXHIM_TRANSPORT_MPI_PACKER

#include <mpi.h>

#include "transport.hpp"
#include "MemoryManagers.hpp"

/**
 * MPIPacker
 * A collection of functions that pack TransportMessages
 * into MPI formatted buffers for transport
 *
 * @param comm    a valid MPI communicator
 * @param message pointer to the mesage that will be packed
 * @param buf     address of the new memory location where the message will be packed into
 * @param bufsize size of the new memory location
*/
class MPIPacker {
    public:
        static int any (const MPI_Comm comm, const TransportMessage         *msg,  void **buf, int *bufsize);

        static int pack(const MPI_Comm comm, const TransportPutMessage      *pm,   void **buf, int *bufsize);
        static int pack(const MPI_Comm comm, const TransportBPutMessage     *bpm,  void **buf, int *bufsize);
        static int pack(const MPI_Comm comm, const TransportGetMessage      *gm,   void **buf, int *bufsize);
        static int pack(const MPI_Comm comm, const TransportBGetMessage     *bgm,  void **buf, int *bufsize);
        static int pack(const MPI_Comm comm, const TransportDeleteMessage   *dm,   void **buf, int *bufsize);
        static int pack(const MPI_Comm comm, const TransportBDeleteMessage  *bdm,  void **buf, int *bufsize);
        static int pack(const MPI_Comm comm, const TransportRecvMessage     *rm,   void **buf, int *bufsize);
        static int pack(const MPI_Comm comm, const TransportGetRecvMessage  *grm,  void **buf, int *bufsize);
        static int pack(const MPI_Comm comm, const TransportBGetRecvMessage *bgrm, void **buf, int *bufsize);
        static int pack(const MPI_Comm comm, const TransportBRecvMessage    *brm,  void **buf, int *bufsize);

    private:
        /**
         * Allocatee the buffer and packs TransportMessage
         * Common to all public functions
         * *bufsize should already have been determined when calling this function
         */
        static int pack(const MPI_Comm comm, const TransportMessage         *msg,  void **buf, int *bufsize, int *position);

        static void cleanup(void **buf, int *bufsize);
};

#endif
