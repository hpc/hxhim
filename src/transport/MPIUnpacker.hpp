#ifndef HXHIM_TRANSPORT_MPI_UNPACKER
#define HXHIM_TRANSPORT_MPI_UNPACKER

#include "mlog2.h"
#include "mlogfacs2.h"
#include <mpi.h>

#include "transport.hpp"
#include "MPIEndpointBase.hpp"

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
        static int any   (const MPIEndpointBase *endpointbase, TransportMessage         **msg,  const void *buf, const int bufsize);

        static int unpack(const MPIEndpointBase *endpointbase, TransportPutMessage      **pm,   const void *buf, const int bufsize);
        static int unpack(const MPIEndpointBase *endpointbase, TransportBPutMessage     **bpm,  const void *buf, const int bufsize);
        static int unpack(const MPIEndpointBase *endpointbase, TransportGetMessage      **gm,   const void *buf, const int bufsize);
        static int unpack(const MPIEndpointBase *endpointbase, TransportBGetMessage     **bgm,  const void *buf, const int bufsize);
        static int unpack(const MPIEndpointBase *endpointbase, TransportDeleteMessage   **dm,   const void *buf, const int bufsize);
        static int unpack(const MPIEndpointBase *endpointbase, TransportBDeleteMessage  **bdm,  const void *buf, const int bufsize);
        static int unpack(const MPIEndpointBase *endpointbase, TransportRecvMessage     **rm,   const void *buf, const int bufsize);
        static int unpack(const MPIEndpointBase *endpointbase, TransportBGetRecvMessage **bgrm, const void *buf, const int bufsize);
        static int unpack(const MPIEndpointBase *endpointbase, TransportBRecvMessage    **brm,  const void *buf, const int bufsize);

    private:
        /**
         * unpack TransportMessage *
         *
         * This function is only called by MPIUnpacker::any
         * to extract the mtype from the packed buffer.
         */
        static int unpack(const MPIEndpointBase *endpointbase, TransportMessage         **msg,  const void *buf, const int bufsize);

        /**
         * unpack TransportMessage *
         *
         * This function can only be called by the other functions.
         * It assumes that the message pointer is valid.
         */
        static int unpack(const MPIEndpointBase *endpointbase, TransportMessage         *msg,   const void *buf, const int bufsize, int *position);
};

#endif
