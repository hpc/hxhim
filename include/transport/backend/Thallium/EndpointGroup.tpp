#if HXHIM_HAVE_THALLIUM

#include <map>

#include <thallium/serialization/stl/string.hpp>

#include "transport/backend/Thallium/Endpoint.hpp"
#include "transport/backend/Thallium/Packer.hpp"
#include "transport/backend/Thallium/Unpacker.hpp"
#include "utils/macros.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

template <typename Recv_t, typename Send_t, typename>
Recv_t *Transport::Thallium::EndpointGroup::do_operation(const std::map<int, Send_t *> &messages) {
    Recv_t *head = nullptr;
    Recv_t *tail = nullptr;

    mlog(THALLIUM_DBG, "Processing Messages in %zu buffers", messages.size());

    for(REF(messages)::value_type const &message : messages) {
        Send_t *msg = message.second;

        mlog(THALLIUM_DBG, "Processing message going to %d", message.first);
        if (!msg) {
            mlog(THALLIUM_DBG, "Message going to %d does not exist", message.first);
            continue;
        }

        mlog(THALLIUM_DBG, "Message is going to range server %d", msg->dst);

        // figure out where to send the message
        std::map<int, Endpoint_t>::const_iterator dst_it = endpoints.find(msg->dst);
        if (dst_it == endpoints.end()) {
            continue;
        }

        // pack the request
        std::string requestbuf;
        if (Packer::pack(msg, requestbuf) != TRANSPORT_SUCCESS) {
            continue;
        }

        mlog(THALLIUM_DBG, "Message going to %d packed into a buffer of size %zu", msg->dst, requestbuf.size());
        mlog(THALLIUM_DBG, "Sending message to %d", message.first);

        // send the message and get a response
        const std::string responsebuf = rpc->on(*dst_it->second)(requestbuf);

        mlog(THALLIUM_DBG, "Received %zu byte response from %d", responsebuf.size(), msg->dst);

        // unpack the response
        Recv_t *response = nullptr;
        if (Unpacker::unpack(&response, responsebuf, responses, arrays, buffers) != TRANSPORT_SUCCESS) {
            continue;
        }

        if (!response) {
            continue;
        }

        if (!head) {
            head = response;
            tail = response;
        }
        else {
            tail->next = response;
            tail = response;
        }

        mlog(THALLIUM_DBG, "Done processing message to %d", msg->dst);
    }

    mlog(THALLIUM_DBG, "Done processing messages");

    return head;
}

#endif
