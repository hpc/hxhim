#ifndef HXHIM_TRANSPORT_MPI_UNPACKER
#define HXHIM_TRANSPORT_MPI_UNPACKER

#include "mlog2.h"
#include "mlogfacs2.h"
#include <mpi.h>

#include "transport.hpp"
#include "MemoryManagers.hpp"
#include "MPIEndpointBase.hpp"

/**
 * MPIUnpacker
 * A collection of functions that unpack
 * MPI formatted buffers into TransportMessages
 *
 * @param comm    a valid MPI communicator
 * @param message address of the pointer that will be created and unpacked into
 * @param buf     the data to convert into the message
 * @param bufsize size of the given data
*/
class MPIUnpacker {
    public:
        static int any   (const MPI_Comm comm, TransportMessage         **msg,  const void *buf, const std::size_t bufsize);

        static int unpack(const MPI_Comm comm, TransportPutMessage      **pm,   const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, TransportBPutMessage     **bpm,  const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, TransportGetMessage      **gm,   const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, TransportBGetMessage     **bgm,  const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, TransportDeleteMessage   **dm,   const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, TransportBDeleteMessage  **bdm,  const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, TransportRecvMessage     **rm,   const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, TransportGetRecvMessage  **grm,  const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, TransportBGetRecvMessage **bgrm, const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, TransportBRecvMessage    **brm,  const void *buf, const std::size_t bufsize);

    private:
        /**
         * unpack TransportMessage *
         *
         * This function is only called by MPIUnpacker::any
         * to extract the mtype from the packed buffer.
         */
        static int unpack(const MPI_Comm comm, TransportMessage         **msg,  const void *buf, const std::size_t bufsize);

        /**
         * unpack TransportMessage *
         *
         * This function can only be called by the other functions.
         * It assumes that the message pointer is valid.
         */
        static int unpack(const MPI_Comm comm, TransportMessage         *msg,   const void *buf, const std::size_t bufsize, int *position);
};

#endif
