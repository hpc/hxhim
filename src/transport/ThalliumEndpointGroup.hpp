#ifndef HXHIM_TRANSPORT_THALLIUM_ENDPOINT_GROUP
#define HXHIM_TRANSPORT_THALLIUM_ENDPOINT_GROUP

#include <map>
#include <mutex>

#include "mlog2.h"
#include "mlogfacs2.h"
#include <thallium.hpp>

#include "mdhim_constants.h"
#include "mdhim_private.h"
#include "transport.hpp"
#include "ThalliumPacker.hpp"
#include "ThalliumUnpacker.hpp"
#include "ThalliumUtilities.hpp"

/**
 * ThalliumEndpointGroup
 * Collective communication endpoint implemented with thallium
 */
class ThalliumEndpointGroup : virtual public TransportEndpointGroup {
    public:
        ThalliumEndpointGroup(mdhim_private_t *mdp);
        ~ThalliumEndpointGroup();

        /** @description Converts a string into an endpoint and adds it to the map_*/
        int AddID(const int id, const Thallium::Engine_t &engine, const std::string &address);

        /** @description Adds an endpoint to the map */
        int AddID(const int id, const Thallium::Endpoint_t &ep);

        /** @description Removes an endpoint*/
        void RemoveID(const int id);

        /** @description Enqueue a BPut requests to multiple endpoints  */
        TransportBRecvMessage *BPut(const int num_rangesrvs, TransportBPutMessage **bpm_list);

        /** @description Enqueue a BGet request to multiple endpoints  */
        TransportBGetRecvMessage *BGet(const int num_rangesrvs, TransportBGetMessage **bgm_list);

        /** @description Bulk Delete to multiple endpoints   */
        TransportBRecvMessage *BDelete(const int num_rangesrvs, TransportBDeleteMessage **bdm_list);

    private:
        /** @description Function used by BPUT and BDELETE for sending and receiving messages */
        TransportBRecvMessage *return_brm(const int num_rangesrvs, TransportMessage **messages);

        /** @description Function used by BGET for sending and receiving messages             */
        TransportBGetRecvMessage *return_bgrm(const int num_rangesrvs, TransportMessage **messages);

        mdhim_private_t *mdp_;
        std::map<int, Thallium::Endpoint_t> endpoints_;
};


#endif
