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
        EndpointGroup(thallium::engine *engine,
                      RangeServer *rs);
        ~EndpointGroup();

        /** @description Converts a string into an endpoint and adds it to the map_*/
        int AddID(const int id, const std::string &address);

        /** @description Adds an endpoint to the map */
        int AddID(const int id, thallium::endpoint *ep);

        /** @description Removes an endpoint*/
        void RemoveID(const int id);

        /** @description Bulk Put to multiple endpoints       */
        Message::Response::BPut *communicate(const ReqList<Message::Request::BPut> &bpm_list);

        /** @description Bulk Get from multiple endpoints     */
        Message::Response::BGet *communicate(const ReqList<Message::Request::BGet> &bgm_list);

        /** @description Bulk Get from multiple endpoints     */
        Message::Response::BGetOp *communicate(const ReqList<Message::Request::BGetOp> &bgm_list);

        /** @description Bulk Delete to multiple endpoints    */
        Message::Response::BDelete *communicate(const ReqList<Message::Request::BDelete> &bdm_list);

        /** @description Bulk Histogram to multiple endpoints */
        Message::Response::BHistogram *communicate(const ReqList<Message::Request::BHistogram> &bhm_list);

    private:
        thallium::engine *engine;                                 /** take ownership */
        RangeServer *rs;                                          /** needed because thats where the rpc signatures are defined */

        std::unordered_map<int, thallium::endpoint *> endpoints;  /** take ownership */
};

}
}

#endif
