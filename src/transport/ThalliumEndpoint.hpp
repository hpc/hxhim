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
        ThalliumEndpoint(const std::string &local_sender_protocol,
                         const std::string &client_receiver_protocol,
                         const std::string &local_receiver_protocol);

        ~ThalliumEndpoint();

        int AddPutRequest(const TransportPutMessage *message);
        int AddGetRequest(const TransportGetMessage *message);

        int AddPutReply(TransportRecvMessage **message);
        int AddGetReply(TransportGetRecvMessage **message);

    private:
        /**
         * Used for sending to the range server
         */
        thallium::engine *sender_;
        const std::string &client_protocol_;

        /**
         * Used for receiving from the range server
         */
        thallium::engine *receiver_;
};

#endif
