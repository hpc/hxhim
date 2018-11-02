#if HXHIM_HAVE_THALLIUM

#include <map>
#include <memory>
#include <vector>

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

    // mlog(THALLIUM_DBG, "Processing Messages in %zu buffers", messages.size());

    for(REF(messages)::value_type const &message : messages) {
        Send_t *msg = message.second;

        if (!msg) {
            // mlog(THALLIUM_DBG, "Message going to %d does not exist", message.first);
            continue;
        }

        // mlog(THALLIUM_DBG, "Message is going to range server %d", msg->dst);

        // figure out where to send the message
        std::map<int, Endpoint_t>::const_iterator dst_it = endpoints.find(msg->dst);
        if (dst_it == endpoints.end()) {
            // mlog(THALLIUM_ERR, "Could not find endpoint for destination rank %d", msg->dst);
            continue;
        }

        // pack the request
        // doing some bad stuff here: the actual allocated buffer should be large enough to allow for responses
        void *buf = nullptr;
        std::size_t bufsize = 0;
        if (Packer::pack(msg, &buf, &bufsize, packed) != TRANSPORT_SUCCESS) {
            // mlog(THALLIUM_DBG, "Unable to pack message");
            continue;
        }

        // mlog(THALLIUM_DBG, "Sending message to %d packed into a buffer of size %zu", msg->dst, bufsize);

        // create the bulk message, send it, and get the size in response
        // the response data is in buf
        std::vector<std::pair<void *, std::size_t> > segments = {std::make_pair(buf, bufsize)};
        thallium::bulk bulk = engine->expose(segments, thallium::bulk_mode::read_write);
        const std::size_t response_size = rpc->on(*dst_it->second)(bulk);
        // mlog(THALLIUM_DBG, "Received %zu byte response from %d", response_size, msg->dst);

        // unpack the response
        Recv_t *response = nullptr;
        const int ret = Unpacker::unpack(&response, buf, response_size, responses, arrays, buffers);
        packed->release(buf, bufsize);
        if (ret != TRANSPORT_SUCCESS) {
            // mlog(THALLIUM_DBG, "Unable to unpack message");
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

        // mlog(THALLIUM_DBG, "Done processing message to %d", msg->dst);
    }

    // mlog(THALLIUM_DBG, "Done processing messages");

    return head;
}

#endif
