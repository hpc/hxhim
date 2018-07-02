#ifndef MDHIM_TRANSPORT_MPI_UNPACKER_HPP
#define MDHIM_TRANSPORT_MPI_UNPACKER_HPP

#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"
#include <mpi.h>

#include "MPIEndpointBase.hpp"
#include "transport.hpp"
#include "utils/MemoryManagers.hpp"

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

        static int unpack(const MPI_Comm comm, TransportRequestMessage  **req,  const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, TransportPutMessage      **pm,   const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, TransportBPutMessage     **bpm,  const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, TransportGetMessage      **gm,   const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, TransportBGetMessage     **bgm,  const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, TransportDeleteMessage   **dm,   const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, TransportBDeleteMessage  **bdm,  const void *buf, const std::size_t bufsize);

        static int unpack(const MPI_Comm comm, TransportResponseMessage **res,  const void *buf, const std::size_t bufsize);
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
         * This function is only be called by unpack TransportRequestMessage and TransportResponseMessage
         */
        static int unpack(const MPI_Comm comm, TransportMessage         *msg,   const void *buf, const std::size_t bufsize, int *position);

        /**
         * unpack TransportRequestMessage *
         *
         * This function is only be called by the functions that unpack buffers containing TransportRequestMessage messages
         */
        static int unpack(const MPI_Comm comm, TransportRequestMessage  *req,  const void *buf, const std::size_t bufsize, int *position);

        /**
         * unpack TransportResponseMessage *
         *
         * This function is only be called by the functions that unpack buffers containing TransportResponseMessage messages
         */
        static int unpack(const MPI_Comm comm, TransportResponseMessage *res,  const void *buf, const std::size_t bufsize, int *position);

        /**
         * unpack TransportRequestMessage *, given a type
         *
         * This function is only be called by MPIUnpacker::any and unpack TransportRequestMessage
         */
        static int unpack(const MPI_Comm comm, TransportRequestMessage  **req, const void *buf, const std::size_t bufsize, const TransportMessageType mtype);

        /**
         * unpack TransportResponseMessage *, given a type
         *
         * This function is only be called by MPIUnpacker::any and unpack TransportResponseMessage
         */
        static int unpack(const MPI_Comm comm, TransportResponseMessage **res, const void *buf, const std::size_t bufsize, const TransportMessageType mtype);
};

#endif
