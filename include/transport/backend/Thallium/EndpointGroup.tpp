#if HXHIM_HAVE_THALLIUM

#include <map>

#include <thallium/serialization/stl/string.hpp>

#include "transport/backend/Thallium/Endpoint.hpp"
#include "transport/backend/Thallium/Packer.hpp"
#include "transport/backend/Thallium/Unpacker.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

template <typename Recv_t, typename Send_t, typename>
Recv_t *Transport::Thallium::EndpointGroup::do_operation(const std::size_t num_rangesrvs, Send_t **messages) {
    Recv_t *head = nullptr;
    Recv_t *tail = nullptr;

    mlog(THALLIUM_DBG, "Processing Messages in %zu buffers", num_rangesrvs);

    for(std::size_t i = 0; i < num_rangesrvs; i++) {
        mlog(THALLIUM_DBG, "Processing Message[%zu]", i);
        if (!messages[i]) {
            mlog(THALLIUM_DBG, "Message[%zu] does not exist", i);
            continue;
        }

        mlog(THALLIUM_DBG, "Message[%zu] is going to range server %d", i, messages[i]->dst);

        // figure out where to send the message
        std::map<int, Endpoint_t>::const_iterator dst_it = endpoints.find(messages[i]->dst);
        if (dst_it == endpoints.end()) {
            continue;
        }

        // pack the request
        std::string requestbuf;
        if (Packer::pack(messages[i], requestbuf) != TRANSPORT_SUCCESS) {
            continue;
        }

        mlog(THALLIUM_DBG, "Message[%zu] packed into a buffer of size %zu", i, requestbuf.size());
        mlog(THALLIUM_DBG, "Sending Message[%zu]", i);

        // send the message and get a response
        const std::string responsebuf = rpc->on(*dst_it->second)(requestbuf);

        mlog(THALLIUM_DBG, "Received %zu byte response for Message[%zu]", responsebuf.size(), i);

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

        mlog(THALLIUM_DBG, "Done processing Message[%zu]", i);
    }

    mlog(THALLIUM_DBG, "Done processing Messages");

    return head;
}

#endif
