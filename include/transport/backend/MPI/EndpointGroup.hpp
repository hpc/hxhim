#ifndef TRANSPORT_MPI_ENDPOINT_GROUP_HPP
#define TRANSPORT_MPI_ENDPOINT_GROUP_HPP

#include <atomic>
#include <map>
#include <pthread.h>

#include <mpi.h>

#include "transport/backend/MPI/EndpointBase.hpp"
#include "transport/transport.hpp"
#include "utils/FixedBufferPool.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

namespace Transport {
namespace MPI {

/**
 * MPIEndpointGroup
 * Collective communication endpoint implemented with MPI
 */
class EndpointGroup : virtual public ::Transport::EndpointGroup, virtual public EndpointBase {
    public:
        EndpointGroup(const MPI_Comm comm,
                      volatile std::atomic_bool &running,
                      std::shared_ptr<FixedBufferPool> packed,
                      FixedBufferPool *responses,
                      FixedBufferPool *arrays,
                      FixedBufferPool *buffers);

        ~EndpointGroup();

        /** @description Add a mapping from a unique ID to a MPI rank */
        void AddID(const int id, const int rank);

        /** @ description Remove a unique ID from the map */
        void RemoveID(const int id);

        /** @description Bulk Put to multiple endpoints    */
        Response::BPut *BPut(const std::size_t num_rangesrvs, Request::BPut **bpm_list);

        /** @description Bulk Get from multiple endpoints  */
        Response::BGet *BGet(const std::size_t num_rangesrvs, Request::BGet **bgm_list);

        /** @description Bulk Get from multiple endpoints  */
        Response::BGetOp *BGetOp(const std::size_t num_rangesrvs, Request::BGetOp **bgm_list);

        /** @description Bulk Delete to multiple endpoints */
        Response::BDelete *BDelete(const std::size_t num_rangesrvs, Request::BDelete **bdm_list);

        /** @description Bulk Histogram to multiple endpoints */
        Response::BHistogram *BHistogram(const std::size_t num_rangesrvs, Request::BHistogram **bhist_list);

    private:
        /** @escription Functions that perform the actual MPI calls */
        template <typename Send_t, typename = std::enable_if_t<std::is_base_of<Request::Request, Send_t>::value &&
                                                               std::is_base_of<Bulk,             Send_t>::value> >
        std::size_t parallel_send(const std::size_t num_srvs, Send_t **messages);                // send to range server

        template <typename Recv_t, typename = std::enable_if_t<std::is_base_of<Response::Response, Recv_t>::value &&
                                                               std::is_base_of<Bulk,               Recv_t>::value> >
        std::size_t parallel_recv(const std::size_t nsrcs, int *srcs, Recv_t ***messages);       // receive from range server

        template <typename Recv_t, typename Send_t, typename = std::enable_if<std::is_base_of<Request::Request,   Send_t>::value &&
                                                                              std::is_base_of<Bulk,               Send_t>::value &&
                                                                              std::is_base_of<Response::Response, Recv_t>::value &&
                                                                              std::is_base_of<Bulk,               Recv_t>::value> >
        Recv_t *return_msgs(const std::size_t num_rangesrvs, Send_t **messages);

        /** @description Mapping from unique ids to MPI ranks */
        std::map<int, int> ranks;

        volatile std::atomic_bool &running;

        /** MPI Specific Allocators */
        FixedBufferPool *mpi_requests;
        FixedBufferPool *ptrs;

        /** Memory that is only allocated once during the lifetime of EndpointGroup
            and is only used by one function at a time */
        std::size_t *lens;
        int *dsts;
        int *srvs;

        /** Allocators from HXHIM */
        FixedBufferPool *responses;
        FixedBufferPool *arrays;
        FixedBufferPool *buffers;
};

}
}

#include "EndpointGroup.tpp"

#endif
