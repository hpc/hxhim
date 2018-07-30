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
        EndpointGroup(const RPC_t &rpc);
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

    private:
        // /** @description Function used by BPUT and BDELETE for sending and receiving messages */
        // TransportBRecvMessage *return_brm(const std::size_t num_rangesrvs, TransportMessage **messages);

        // /** @description Function used by BGET for sending and receiving messages             */
        // TransportBGetRecvMessage *return_bgrm(const std::size_t num_rangesrvs, TransportMessage **messages);

        RPC_t rpc_;

        std::map<int, Endpoint_t> endpoints_;
};

}
}

#endif
