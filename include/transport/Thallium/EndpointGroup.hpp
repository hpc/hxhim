#if HXHIM_HAVE_THALLIUM

#ifndef TRANSPORT_THALLIUM_ENDPOINT_GROUP_HPP
#define TRANSPORT_THALLIUM_ENDPOINT_GROUP_HPP

#include <unordered_map>

#include "transport/Thallium/Utilities.hpp"
#include "transport/transport.hpp"

namespace Transport {
namespace Thallium {

/**
 * EndpointGroup
 * Collective communication endpoint implemented with thallium
 */
class EndpointGroup : virtual public ::Transport::EndpointGroup {
    public:
        EndpointGroup(const Engine_t &engine,
                      const RPC_t &rpc,
                      const std::size_t buffer_size);
        ~EndpointGroup();

        /** @description Converts a string into an endpoint and adds it to the map_*/
        int AddID(const int id, const Engine_t &engine, const std::string &address);

        /** @description Adds an endpoint to the map */
        int AddID(const int id, const Endpoint_t &ep);

        /** @description Removes an endpoint*/
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
        Engine_t engine;
        RPC_t rpc;
        const std::size_t buffer_size;

        std::unordered_map<int, Endpoint_t> endpoints;
};

}
}

#endif

#endif
