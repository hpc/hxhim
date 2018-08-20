#ifndef TRANSPORT_MPI_PACKER_HPP
#define TRANSPORT_MPI_PACKER_HPP

#include <mpi.h>

#include "transport/Messages/Messages.hpp"
#include "utils/FixedBufferPool.hpp"

namespace Transport {
namespace MPI {

/**
 * MPIPacker
 * A collection of functions that pack Messages
 * into formatted buffers for transport with MPI
 *
 * @param comm    a valid MPI communicator
 * @param message pointer to the mesage that will be packed
 * @param buf     address of the new memory location where the message will be packed into
 * @param bufsize size of the new memory location
*/
class Packer {
    public:
        static int any (const MPI_Comm comm, const Message              *msg,   void **buf, std::size_t *bufsize, FixedBufferPool *fbp);

        static int pack(const MPI_Comm comm, const Request::Request     *req,   void **buf, std::size_t *bufsize, FixedBufferPool *fbp);
        static int pack(const MPI_Comm comm, const Request::Put         *pm,    void **buf, std::size_t *bufsize, FixedBufferPool *fbp);
        static int pack(const MPI_Comm comm, const Request::Get         *gm,    void **buf, std::size_t *bufsize, FixedBufferPool *fbp);
        static int pack(const MPI_Comm comm, const Request::Delete      *dm,    void **buf, std::size_t *bufsize, FixedBufferPool *fbp);
        static int pack(const MPI_Comm comm, const Request::Histogram   *hist,  void **buf, std::size_t *bufsize, FixedBufferPool *fbp);
        static int pack(const MPI_Comm comm, const Request::BPut        *bpm,   void **buf, std::size_t *bufsize, FixedBufferPool *fbp);
        static int pack(const MPI_Comm comm, const Request::BGet        *bgm,   void **buf, std::size_t *bufsize, FixedBufferPool *fbp);
        static int pack(const MPI_Comm comm, const Request::BGetOp      *bgm,   void **buf, std::size_t *bufsize, FixedBufferPool *fbp);
        static int pack(const MPI_Comm comm, const Request::BDelete     *bdm,   void **buf, std::size_t *bufsize, FixedBufferPool *fbp);
        static int pack(const MPI_Comm comm, const Request::BHistogram  *bhist, void **buf, std::size_t *bufsize, FixedBufferPool *fbp);

        static int pack(const MPI_Comm comm, const Response::Response   *res,   void **buf, std::size_t *bufsize, FixedBufferPool *fbp);
        static int pack(const MPI_Comm comm, const Response::Put        *pm,    void **buf, std::size_t *bufsize, FixedBufferPool *fbp);
        static int pack(const MPI_Comm comm, const Response::Get        *gm,    void **buf, std::size_t *bufsize, FixedBufferPool *fbp);
        static int pack(const MPI_Comm comm, const Response::Delete     *dm,    void **buf, std::size_t *bufsize, FixedBufferPool *fbp);
        static int pack(const MPI_Comm comm, const Response::Histogram  *hist,  void **buf, std::size_t *bufsize, FixedBufferPool *fbp);
        static int pack(const MPI_Comm comm, const Response::BPut       *bpm,   void **buf, std::size_t *bufsize, FixedBufferPool *fbp);
        static int pack(const MPI_Comm comm, const Response::BGet       *bgm,   void **buf, std::size_t *bufsize, FixedBufferPool *fbp);
        static int pack(const MPI_Comm comm, const Response::BGetOp     *bgm,   void **buf, std::size_t *bufsize, FixedBufferPool *fbp);
        static int pack(const MPI_Comm comm, const Response::BDelete    *bdm,   void **buf, std::size_t *bufsize, FixedBufferPool *fbp);
        static int pack(const MPI_Comm comm, const Response::BHistogram *bhist, void **buf, std::size_t *bufsize, FixedBufferPool *fbp);

    private:
        /**
         * Allocatee the buffer and packs TransportMessage
         * Common to all public functions
         * *bufsize should already have been determined when calling this function
         */
        static int pack(const MPI_Comm comm, const Message              *msg,   void **buf, std::size_t *bufsize, int *position, FixedBufferPool *fbp);
        static int pack(const MPI_Comm comm, const Request::Request     *req,   void **buf, std::size_t *bufsize, int *position, FixedBufferPool *fbp);
        static int pack(const MPI_Comm comm, const Response::Response   *res,   void **buf, std::size_t *bufsize, int *position, FixedBufferPool *fbp);

        static void cleanup(void **buf, std::size_t *bufsize, FixedBufferPool *fbp);
};

}
}

#endif
