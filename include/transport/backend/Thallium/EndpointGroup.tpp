#if HXHIM_HAVE_THALLIUM

#include <map>

#include "transport/backend/Thallium/Endpoint.hpp"
#include "transport/backend/Thallium/Packer.hpp"
#include "transport/backend/Thallium/Unpacker.hpp"

template <typename Recv_t, typename Send_t, typename>
Recv_t *Transport::Thallium::EndpointGroup::do_operation(const std::size_t num_rangesrvs, Send_t **messages) {
    Recv_t *head = nullptr;
    Recv_t *tail = nullptr;

    for(std::size_t i = 0; i < num_rangesrvs; i++) {
        if (!messages[i]) {
            continue;
        }

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

        // send the message and get a response
        const std::string responsebuf = rpc->on(*dst_it->second)(requestbuf);

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
    }

    return head;
}

#endif
