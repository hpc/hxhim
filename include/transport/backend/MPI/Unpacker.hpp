#ifndef TRANSPORT_MPI_UNPACKER_HPP
#define TRANSPORT_MPI_UNPACKER_HPP

#include <mpi.h>

#include "transport/Messages.hpp"
#include "utils/MemoryManagers.hpp"

namespace Transport {
namespace MPI {

/**
 * MPIUnpacker
 * A collection of functions that unpack
 * MPI formatted buffers into Messages
 *
 * @param comm    a valid MPI communicator
 * @param message address of the pointer that will be created and unpacked into
 * @param buf     the data to convert into the message
 * @param bufsize size of the given data
*/
class Unpacker {
    public:
        static int any   (const MPI_Comm comm, Message              **msg,   const void *buf, const std::size_t bufsize);

        static int unpack(const MPI_Comm comm, Request::Request     **req,   const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, Request::Put         **pm,    const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, Request::Get         **gm,    const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, Request::Delete      **dm,    const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, Request::Histogram   **hist,  const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, Request::BPut        **bpm,   const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, Request::BGet        **bgm,   const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, Request::BGetOp      **bgm,   const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, Request::BDelete     **bdm,   const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, Request::BHistogram  **bhist, const void *buf, const std::size_t bufsize);

        static int unpack(const MPI_Comm comm, Response::Response   **res,   const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, Response::Put        **pm,    const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, Response::Get        **gm,    const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, Response::Delete     **dm,    const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, Response::Histogram  **hist,  const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, Response::BPut       **bpm,   const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, Response::BGet       **bgm,   const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, Response::BGetOp     **bgm,   const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, Response::BDelete    **bdm,   const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, Response::BHistogram **bhist, const void *buf, const std::size_t bufsize);

    private:
        static int unpack(const MPI_Comm comm, Message              *msg,    const void *buf, const std::size_t bufsize, int *position);
        static int unpack(const MPI_Comm comm, Request::Request     *req,    const void *buf, const std::size_t bufsize, int *position);
        static int unpack(const MPI_Comm comm, Response::Response   *res,    const void *buf, const std::size_t bufsize, int *position);

        static int unpack(const MPI_Comm comm, Message              **msg,   const void *buf, const std::size_t bufsize);
        static int unpack(const MPI_Comm comm, Request::Request     **req,   const void *buf, const std::size_t bufsize, const Message::Type type);
        static int unpack(const MPI_Comm comm, Response::Response   **res,   const void *buf, const std::size_t bufsize, const Message::Type type);

};

}
}

#endif
