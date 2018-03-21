#ifndef HXHIM_TRANSPORT_THALLIUM_ENDPOINT
#define HXHIM_TRANSPORT_THALLIUM_ENDPOINT

#include <map>

#include <thallium.hpp>

#include "mdhim_constants.h"
#include "transport.hpp"
#include "ThalliumPacker.hpp"
#include "ThalliumUnpacker.hpp"

/**
 * ThalliumAddress
 * Point-to-Point communication endpoint implemented with thallium
 */
class ThalliumEndpoint : virtual public TransportEndpoint {
    public:
        /** Create a TransportEndpoint for a specified process */
        ThalliumEndpoint(const std::string &sender_protocol,
                         const std::string &receiver_protocol);

        /** Destructor */
        ~ThalliumEndpoint();

        int AddPutRequest(const TransportPutMessage *message);
        int AddGetRequest(const TransportGetMessage *message);

        int AddPutReply(TransportRecvMessage **message);
        int AddGetReply(TransportGetRecvMessage **message);

    private:
        thallium::engine *sender_;
        const std::string &receiver_protocol_;
};

#endif
