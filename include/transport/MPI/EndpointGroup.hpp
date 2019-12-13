#ifndef TRANSPORT_MPI_ENDPOINT_GROUP_HPP
#define TRANSPORT_MPI_ENDPOINT_GROUP_HPP

#include <atomic>
#include <unordered_map>

#include <mpi.h>

#include "transport/MPI/EndpointBase.hpp"
#include "transport/transport.hpp"
#include "utils/enable_if_t.hpp"
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
                      volatile std::atomic_bool &running);

        ~EndpointGroup();

        /** @description Add a mapping from a unique ID to a MPI rank */
        void AddID(const int id, const int rank);

        /** @ description Remove a unique ID from the map */
        void RemoveID(const int id);

        /** @description Bulk Put to multiple endpoints    */
        Response::RecvBPut *communicate(const std::unordered_map<int, Request::SendBPut *> &bpm_list);

        /** @description Bulk Get from multiple endpoints  */
        Response::RecvBGet *communicate(const std::unordered_map<int, Request::SendBGet *> &bgm_list);

        /** @description Bulk Get from multiple endpoints  */
        Response::RecvBGetOp *communicate(const std::unordered_map<int, Request::SendBGetOp *> &bgm_list);

        /** @description Bulk Delete to multiple endpoints */
        Response::RecvBDelete *communicate(const std::unordered_map<int, Request::SendBDelete *> &bdm_list);

        // /** @description Bulk Histogram to multiple endpoints */
        // Response::RecvBHistogram *communicate(const std::unordered_map<int, Request::SendBHistogram *> &bhist_list);

    private:
        /** @escription Functions that perform the actual MPI calls */
        template <typename Send_t, typename = enable_if_t<std::is_base_of<Request::SendRequest, Send_t>::value &&
                                                          std::is_base_of<Bulk,             Send_t>::value> >
        std::size_t parallel_send(const std::unordered_map<int, Send_t *> &messages);            // send to range server

        template <typename Recv_t, typename = enable_if_t<std::is_base_of<Response::RecvResponse, Recv_t>::value &&
                                                          std::is_base_of<Bulk,               Recv_t>::value> >
        std::size_t parallel_recv(const std::size_t nsrcs, int *srcs, Recv_t ***messages);       // receive from range server

        template <typename Recv_t, typename Send_t, typename = enable_if_t<std::is_base_of<Request::SendRequest,   Send_t>::value &&
                                                                           std::is_base_of<Bulk,               Send_t>::value &&
                                                                           std::is_base_of<Response::RecvResponse, Recv_t>::value &&
                                                                           std::is_base_of<Bulk,               Recv_t>::value> >
        Recv_t *return_msgs(const std::unordered_map<int, Send_t *> &messages);

        /** @description Mapping from unique ids to MPI ranks */
        std::unordered_map<int, int> ranks;

        volatile std::atomic_bool &running;

        /** Memory that is only allocated once during the lifetime of EndpointGroup
            and is only used by one function at a time */
        std::size_t *lens; // buffer lengths
        int *dsts;         // request destination servers
        int *srvs;         // response source servers
};

}
}

#include "EndpointGroup.tpp"

#endif
