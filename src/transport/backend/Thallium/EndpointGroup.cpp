#include "transport/backend/Thallium/EndpointGroup.hpp"
#include "transport/backend/Thallium/Packer.hpp"
#include "transport/backend/Thallium/Unpacker.hpp"

Transport::Thallium::EndpointGroup::EndpointGroup(const Thallium::RPC_t &rpc)
    : ::Transport::EndpointGroup(),
      rpc_(rpc),
      endpoints_()
{}

Transport::Thallium::EndpointGroup::~EndpointGroup() {}

/**
 * AddID
 * Add a mapping from a unique ID to a thalllium address
 * ThalliumEndpointGroup takes ownership of ep
 *     - If the engine is null or if the lookup fails,
 *       nothing is done and TRANSPORT_ERROR is returned
 *
 * @param id       the unique ID associated with the rank
 * @param engine   the engine instance
 * @param address  the address of the other end
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR
 */
int Transport::Thallium::EndpointGroup::AddID(const int id, const Thallium::Engine_t &engine, const std::string &address) {
    if (!engine) {
        return TRANSPORT_ERROR;
    }

    Thallium::Endpoint_t ep(new thallium::endpoint(engine->lookup(address)));
    if (!ep) {
        return TRANSPORT_ERROR;
    }

    return AddID(id, ep);
}

/**
 * AddID
 * Add a mapping from a unique ID to a MPI rank
 * ThalliumEndpointGroup takes ownership of ep
 *     - If ep is null, nothing is done
 *       and TRANSPORT_ERROR is returned
 *     - If there is already an endpoint at id,
 *       the endpoint is destroyed
 *
 * @param id the unique ID associated with the rank
 * @param ep the other end
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR
 */
int Transport::Thallium::EndpointGroup::AddID(const int id, const Thallium::Endpoint_t &ep) {
    if (!ep) {
        return TRANSPORT_ERROR;
    }

    endpoints_[id] = ep;
    return TRANSPORT_SUCCESS;
}

/**
 * RemoveID
 * Remove a mapping from a unique ID to a rank
 * If the unique ID is not found, nothing is done
 *
 * @param id the unique ID to remove
 */
void Transport::Thallium::EndpointGroup::RemoveID(const int id) {
    endpoints_.erase(id);
}

/**
 * BPut
 *
 * @param num_rangesrvs the total number of range servers
 * @param bpm_list the list of BPUT messages to send
 * @return a linked list of response messages, or nullptr
 */
Transport::Response::BPut *Transport::Thallium::EndpointGroup::BPut(const std::size_t num_rangesrvs, Request::BPut **bpm_list) {
    // convert the responses into a list
    Response::BPut *head = nullptr;
    Response::BPut *tail = head;

    // encode each mesage
    for(std::size_t i = 0; i < num_rangesrvs; i++) {
        if (!bpm_list[i]) {
            continue;
        }

        // pack message
        std::string sendbuf;
        if (Thallium::Packer::any(bpm_list[i], sendbuf) != TRANSPORT_SUCCESS) {
            continue;
        }

        std::map<int, Thallium::Endpoint_t>::const_iterator dst_it = endpoints_.find(bpm_list[i]->dst);
        if (dst_it == endpoints_.end()) {
            continue;
        }

        // send message and receive response
        const std::string response = rpc_->on(*dst_it->second)(sendbuf);

        // unpack response
        Response::BPut *res = nullptr;
        if (Thallium::Unpacker::unpack(&res, response) != TRANSPORT_SUCCESS) {
            continue;
        }

        //Build the linked list to return
        if (!head) {
            head = res;
            tail = res;
        } else {
            tail->next = res;
            tail = res;
        }

        while (tail->next) {
            tail = tail->next;
        }
    }

    return head;
}

/**
 * BGet
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list the list of BGET messages to send
 * @return a linked list of response messages, or nullptr
 */
Transport::Response::BGet *Transport::Thallium::EndpointGroup::BGet(const std::size_t num_rangesrvs, Request::BGet **bgm_list) {
    // Message **messages = convert_to_base(num_rangesrvs, bgm_list);
    // TransportBGetRecvMessage *ret = return_bgrm(num_rangesrvs, messages);
    // delete [] messages;
    // return ret;
    return nullptr;
}

/**
 * BGetOp
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list the list of BGETOP messages to send
 * @return a linked list of response messages, or nullptr
 */
Transport::Response::BGetOp *Transport::Thallium::EndpointGroup::BGetOp(const std::size_t num_rangesrvs, Request::BGetOp **bgm_list) {
    // Message **messages = convert_to_base(num_rangesrvs, bgm_list);
    // TransportBGetRecvMessage *ret = return_bgrm(num_rangesrvs, messages);
    // delete [] messages;
    // return ret;
    return nullptr;
}

/**
 * BDelete
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list the list of BDELETE messages to send
 * @return a linked list of response messages, or nullptr
 */
Transport::Response::BDelete *Transport::Thallium::EndpointGroup::BDelete(const std::size_t num_rangesrvs, Request::BDelete **bdm_list) {
    // Message **messages = convert_to_base(num_rangesrvs, bdm_list);
    // Transport::Response::BDelete *ret = return_brm(num_rangesrvs, messages);
    // delete [] messages;
    // return ret;
    return nullptr;
}

/**
 * BHistogram
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list the list of BDELETE messages to send
 * @return a linked list of response messages, or nullptr
 */
Transport::Response::BHistogram *Transport::Thallium::EndpointGroup::BHistogram(const std::size_t num_rangesrvs, Request::BHistogram **bhist_list) {
    // Message **messages = convert_to_base(num_rangesrvs, bhist_list);
    // Transport::Response::BHistogram *ret = return_brm(num_rangesrvs, messages);
    // delete [] messages;
    // return ret;
    return nullptr;
}

// /**
//  * return_brm
//  *
//  * @param num_rangesrvs the number of range servers there are
//  * @param messages      an array of messages to send
//  * @return a linked list of return messages
//  */
// TransportBRecvMessage *Transport::Thallium::EndpointGroup::return_brm(const std::size_t num_rangesrvs, TransportMessage **messages) {
//     // convert the responses into a list
//     TransportBRecvMessage *head = nullptr;
//     TransportBRecvMessage *tail = nullptr;

//     // encode each mesage
//     for(std::size_t i = 0; i < num_rangesrvs; i++) {
//         if (!messages[i]) {
//             continue;
//         }

//         // pack message
//         std::string sendbuf;
//         if (Thallium::Packer::any(messages[i], sendbuf) != TRANSPORT_SUCCESS) {
//             continue;
//         }

//         std::map<int, Thallium::Endpoint_t>::const_iterator dst_it = endpoints_.find(messages[i]->dst);
//         if (dst_it != endpoints_.end()) {
//             // send message and receive response
//             const std::string response = rpc_->on(*dst_it->second)(sendbuf);

//             // unpack response
//             TransportRecvMessage *rm = nullptr;
//             if (Thallium::Unpacker::unpack(&rm, response) != TRANSPORT_SUCCESS) {
//                 continue;
//             }

//             if (!rm) {
//                 //Skip this as the message doesn't exist
//                 continue;
//             }

//             // convert response into a TransportBRecvMessage
//             TransportBRecvMessage *brm = new TransportBRecvMessage();
//             brm->error = rm->error;
//             brm->src = rm->src;
//             brm->dst = rm->dst;
//             brm->next = nullptr;
//             delete rm;

//             //Build the linked list to return
//             if (!head) {
//                 head = brm;
//                 tail = brm;
//             } else {
//                 tail->next = brm;
//                 tail = brm;
//             }
//         }
//     }

//     return head;
// }

// /**
//  * return_bgrm
//  * This function is very similar to return_brm
//  *
//  * @param num_rangesrvs the number of range servers there are
//  * @param messages      an array of messages to send
//  * @return a linked list of return messages
//  */
// TransportBGetRecvMessage *Transport::Thallium::EndpointGroup::return_bgrm(const std::size_t num_rangesrvs, TransportMessage **messages) {
//     // convert the responses into a list
//     TransportBGetRecvMessage *head = nullptr;
//     TransportBGetRecvMessage *tail = nullptr;

//     for(std::size_t i = 0; i < num_rangesrvs; i++) {
//         if (!messages[i]) {
//             continue;
//         }

//         std::map<int, Thallium::Endpoint_t>::const_iterator dst_it = endpoints_.find(messages[i]->dst);
//         if (dst_it != endpoints_.end()) {
//             // pack message
//             std::string sendbuf;
//             if (Thallium::Packer::any(messages[i], sendbuf) != TRANSPORT_SUCCESS) {
//                 continue;
//             }

//             // send message and receive response
//             const std::string response = rpc_->on(*dst_it->second)(sendbuf);

//             // unpack response
//             TransportBGetRecvMessage *bgrm = nullptr;
//             if (Thallium::Unpacker::unpack(&bgrm, response) != TRANSPORT_SUCCESS) {
//                 continue;
//             }

//             if (!bgrm) {
//                 //Skip this as the message doesn't exist
//                 continue;
//             }

//             //Build the linked list to return
//             bgrm->next = nullptr;
//             if (!head) {
//                 head = bgrm;
//                 tail = bgrm;
//             } else {
//                 tail->next = bgrm;
//                 tail = bgrm;
//             }
//         }
//     }

//     return head;
// }
