#if HXHIM_HAVE_THALLIUM

#ifndef TRANSPORT_THALLIUM_ENDPOINT_GROUP_HPP
#define TRANSPORT_THALLIUM_ENDPOINT_GROUP_HPP

#include <map>

#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"
#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

#include "transport/backend/Thallium/Utilities.hpp"
#include "transport/transport.hpp"

namespace Transport {
namespace Thallium {

/**
 * EndpointGroup
 * Collective communication endpoint implemented with thallium
 */
class EndpointGroup : virtual public ::Transport::EndpointGroup {
    public:
        EndpointGroup(const RPC_t &rpc,
                      FixedBufferPool *packed,
                      FixedBufferPool *responses,
                      FixedBufferPool *arrays,
                      FixedBufferPool *buffers);
        ~EndpointGroup();

        /** @description Converts a string into an endpoint and adds it to the map_*/
        int AddID(const int id, const Engine_t &engine, const std::string &address);

        /** @description Adds an endpoint to the map */
        int AddID(const int id, const Endpoint_t &ep);

        /** @description Removes an endpoint*/
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
        template <typename Recv_t, typename Send_t, typename = std::enable_if<std::is_base_of<Request::Request,   Send_t>::value &&
                                                                              std::is_base_of<Bulk,               Send_t>::value &&
                                                                              std::is_base_of<Response::Response, Recv_t>::value &&
                                                                              std::is_base_of<Bulk,               Recv_t>::value> >
        Recv_t *do_operation(const std::size_t num_rangesrvs, Send_t **messages);

        RPC_t rpc;

        std::map<int, Endpoint_t> endpoints;

        FixedBufferPool *packed;
        FixedBufferPool *responses;
        FixedBufferPool *arrays;
        FixedBufferPool *buffers;
};

}
}

#include "transport/backend/Thallium/EndpointGroup.tpp"

#endif

#endif
