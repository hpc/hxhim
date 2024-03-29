#ifndef TRANSPORT_MPI_ENDPOINT_GROUP_HPP
#define TRANSPORT_MPI_ENDPOINT_GROUP_HPP

#include <atomic>
#include <unordered_map>

#include <mpi.h>

#include "transport/backend/MPI/EndpointBase.hpp"
#include "transport/transport.hpp"
#include "utils/type_traits.hpp"
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
        Message::Response::BPut *communicate(const ReqList<Message::Request::BPut> &bpm_list);

        /** @description Bulk Get from multiple endpoints  */
        Message::Response::BGet *communicate(const ReqList<Message::Request::BGet> &bgm_list);

        /** @description Bulk Get from multiple endpoints  */
        Message::Response::BGetOp *communicate(const ReqList<Message::Request::BGetOp> &bgm_list);

        /** @description Bulk Delete to multiple endpoints */
        Message::Response::BDelete *communicate(const ReqList<Message::Request::BDelete> &bdm_list);

        /** @description Bulk Histogram to multiple endpoints */
        Message::Response::BHistogram *communicate(const ReqList<Message::Request::BHistogram> &bhm_list);

    private:
        /** @escription Functions that perform the actual MPI calls */
        template <typename Send_t, typename = enable_if_t<std::is_base_of<Message::Request::Request, Send_t>::value> >
        std::size_t parallel_send(const ReqList<Send_t> &messages);                         // send to range server

        template <typename Recv_t, typename = enable_if_t<std::is_base_of<Message::Response::Response, Recv_t>::value> >
        std::size_t parallel_recv(const std::size_t nsrcs, int *srcs, Recv_t ***messages);  // receive from range server

        template <typename Recv_t, typename Send_t,
                  typename = enable_if_t<std::is_base_of<Message::Request::Request,   Send_t>::value &&
                                         std::is_base_of<Message::Response::Response, Recv_t>::value> >
        Recv_t *return_msgs(const ReqList<Send_t> &messages);

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
