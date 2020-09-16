#ifndef TRANSPORT_THALLIUM_ENDPOINT_GROUP_HPP
#define TRANSPORT_THALLIUM_ENDPOINT_GROUP_HPP

#include <unordered_map>

#include "transport/backend/Thallium/RangeServer.hpp"
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
        EndpointGroup(const Engine_t &engine,
                      RangeServer *rs);
        ~EndpointGroup();

        /** @description Converts a string into an endpoint and adds it to the map_*/
        int AddID(const int id, const Engine_t &engine, const std::string &address);

        /** @description Adds an endpoint to the map */
        int AddID(const int id, const Endpoint_t &ep);

        /** @description Removes an endpoint*/
        void RemoveID(const int id);

        /** @description Bulk Put to multiple endpoints       */
        Response::BPut *communicate(const ReqList<Request::BPut> &bpm_list);

        /** @description Bulk Get from multiple endpoints     */
        Response::BGet *communicate(const ReqList<Request::BGet> &bgm_list);

        /** @description Bulk Get from multiple endpoints     */
        Response::BGetOp *communicate(const ReqList<Request::BGetOp> &bgm_list);

        /** @description Bulk Delete to multiple endpoints    */
        Response::BDelete *communicate(const ReqList<Request::BDelete> &bdm_list);

        /** @description Bulk Histogram to multiple endpoints */
        Response::BHistogram *communicate(const ReqList<Request::BHistogram> &bhm_list);

    private:
        Engine_t engine;
        RangeServer *rs; /** needed because thats where the rpc signatures are defined */

        std::unordered_map<int, Endpoint_t> endpoints;
};

}
}

#endif
