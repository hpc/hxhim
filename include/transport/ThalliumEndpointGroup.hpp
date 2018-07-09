#ifndef MDHIM_TRANSPORT_THALLIUM_ENDPOINT_GROUP_HPP
#define MDHIM_TRANSPORT_THALLIUM_ENDPOINT_GROUP_HPP

#include <map>

#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"
#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

#include "ThalliumPacker.hpp"
#include "ThalliumUnpacker.hpp"
#include "ThalliumUtilities.hpp"
#include "mdhim/constants.h"
#include "mdhim/private.h"
#include "transport.hpp"

/**
 * ThalliumEndpointGroup
 * Collective communication endpoint implemented with thallium
 */
class ThalliumEndpointGroup : virtual public TransportEndpointGroup {
    public:
        ThalliumEndpointGroup(const Thallium::RPC_t &rpc);
        ~ThalliumEndpointGroup();

        /** @description Converts a string into an endpoint and adds it to the map_*/
        int AddID(const int id, const Thallium::Engine_t &engine, const std::string &address);

        /** @description Adds an endpoint to the map */
        int AddID(const int id, const Thallium::Endpoint_t &ep);

        /** @description Removes an endpoint*/
        void RemoveID(const int id);

        /** @description Enqueue a BPut requests to multiple endpoints  */
        TransportBRecvMessage *BPut(const std::size_t num_rangesrvs, TransportBPutMessage **bpm_list);

        /** @description Enqueue a BGet request to multiple endpoints  */
        TransportBGetRecvMessage *BGet(const std::size_t num_rangesrvs, TransportBGetMessage **bgm_list);

        /** @description Bulk Delete to multiple endpoints             */
        TransportBRecvMessage *BDelete(const std::size_t num_rangesrvs, TransportBDeleteMessage **bdm_list);

    private:
        /** @description Function used by BPUT and BDELETE for sending and receiving messages */
        TransportBRecvMessage *return_brm(const std::size_t num_rangesrvs, TransportMessage **messages);

        /** @description Function used by BGET for sending and receiving messages             */
        TransportBGetRecvMessage *return_bgrm(const std::size_t num_rangesrvs, TransportMessage **messages);

        Thallium::RPC_t rpc_;

        std::map<int, Thallium::Endpoint_t> endpoints_;
};

#endif