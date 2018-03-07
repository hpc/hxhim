#ifndef HXHIM_TRANSPORT_MPI_UNPACKER
#define HXHIM_TRANSPORT_MPI_UNPACKER

#include "mlog2.h"
#include "mlogfacs2.h"
#include <mpi.h>

#include "transport.hpp"
#include "MPITransportBase.hpp"

/**
 * MPIPack
 * A collection of functions that unpack
 * MPI formatted buffers into TransportMessages
 *
 * @param comm       a valid MPI communicator
 * @param **message  address to the pointer that will be created and unpacked into
 * @param buf        the data to convert into the message
 * @param bufsize    size of the given data
*/
class MPIUnpacker {
    public:
        static int any   (const MPITransportBase *transportbase, TransportMessage         **msg,  const void *buf, const int bufsize);

        static int unpack(const MPITransportBase *transportbase, TransportPutMessage      **pm,   const void *buf, const int bufsize);
        static int unpack(const MPITransportBase *transportbase, TransportBPutMessage     **bpm,  const void *buf, const int bufsize);
        static int unpack(const MPITransportBase *transportbase, TransportGetMessage      **gm,   const void *buf, const int bufsize);
        static int unpack(const MPITransportBase *transportbase, TransportBGetMessage     **bgm,  const void *buf, const int bufsize);
        static int unpack(const MPITransportBase *transportbase, TransportDeleteMessage   **dm,   const void *buf, const int bufsize);
        static int unpack(const MPITransportBase *transportbase, TransportBDeleteMessage  **bdm,  const void *buf, const int bufsize);
        static int unpack(const MPITransportBase *transportbase, TransportRecvMessage     **rm,   const void *buf, const int bufsize);
        static int unpack(const MPITransportBase *transportbase, TransportBGetRecvMessage **bgrm, const void *buf, const int bufsize);
        static int unpack(const MPITransportBase *transportbase, TransportBRecvMessage    **brm,  const void *buf, const int bufsize);

    private:
        /**
         * unpack TransportMessage *
         *
         * This function is only called by MPIUnpacker::any
         * to extract the mtype from the packed buffer.
         */
        static int unpack(const MPITransportBase *transportbase, TransportMessage         **msg,  const void *buf, const int bufsize);

        /**
         * unpack TransportMessage *
         *
         * This function can only be called by the other functions.
         * It assumes that the message pointer is valid.
         */
        static int unpack(const MPITransportBase *transportbase, TransportMessage         *msg,   const void *buf, const int bufsize, int *position);
};

#endif
