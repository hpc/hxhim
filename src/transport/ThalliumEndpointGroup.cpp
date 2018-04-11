#include "ThalliumEndpointGroup.hpp"

ThalliumEndpointGroup::ThalliumEndpointGroup(mdhim_private_t *mdp, const Thallium::RPC_t &rpc)
    : TransportEndpointGroup(),
      mdp_(mdp),
      rpc_(rpc),
      endpoints_()
{}

ThalliumEndpointGroup::~ThalliumEndpointGroup() {}

/**
 * AddID
 * Add a mapping from a unique ID to a thalllium address
 * ThalliumEndpointGroup takes ownership of ep
 *     - If the engine is null or if the lookup fails,
 *       nothing is done and MDHIM_ERROR is returned
 *
 * @param id       the unique ID associated with the rank
 * @param engine   the engine instance
 * @param address  the address of the other end
 * @return MDHIM_SUCCESS or MDHIM_ERROR
 */
int ThalliumEndpointGroup::AddID(const int id, const Thallium::Engine_t &engine, const std::string &address) {
    if (!engine) {
        return MDHIM_ERROR;
    }

    Thallium::Endpoint_t ep(new thallium::endpoint(engine->lookup(address)));
    if (!ep) {
        return MDHIM_ERROR;
    }

    return AddID(id, ep);
}

/**
 * AddID
 * Add a mapping from a unique ID to a MPI rank
 * ThalliumEndpointGroup takes ownership of ep
 *     - If ep is null, nothing is done
 *       and MDHIM_ERROR is returned
 *     - If there is already an endpoint at id,
 *       the endpoint is destroyed
 *
 * @param id the unique ID associated with the rank
 * @param ep the other end
 * @return MDHIM_SUCCESS or MDHIM_ERROR
 */
int ThalliumEndpointGroup::AddID(const int id, const Thallium::Endpoint_t &ep) {
    if (!ep) {
        return MDHIM_ERROR;
    }

    endpoints_[id] = ep;
    return MDHIM_SUCCESS;
}

/**
 * RemoveID
 * Remove a mapping from a unique ID to a rank
 * If the unique ID is not found, nothing is done
 *
 * @param id the unique ID to remove
 */
void ThalliumEndpointGroup::RemoveID(const int id) {
    endpoints_.erase(id);
}

/**
 * BPut
 *
 * @param num_rangesrvs the total number of range servers
 * @param bpm_list the list of BPUT messages to send
 * @return a linked list of response messages, or nullptr
 */
TransportBRecvMessage *ThalliumEndpointGroup::BPut(const int num_rangesrvs, TransportBPutMessage **bpm_list) {
    TransportMessage **messages = convert_to_base(num_rangesrvs, bpm_list);
    TransportBRecvMessage *ret = return_brm(num_rangesrvs, messages);
    delete [] messages;
    return ret;
}

/**
 * BGet
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list the list of BGET messages to send
 * @return a linked list of response messages, or nullptr
 */
TransportBGetRecvMessage *ThalliumEndpointGroup::BGet(const int num_rangesrvs, TransportBGetMessage **bgm_list) {
    TransportMessage **messages = convert_to_base(num_rangesrvs, bgm_list);
    TransportBGetRecvMessage *ret = return_bgrm(num_rangesrvs, messages);
    delete [] messages;
    return ret;
}

/**
 * BDelete
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list the list of BDELETE messages to send
 * @return a linked list of response messages, or nullptr
 */
TransportBRecvMessage *ThalliumEndpointGroup::BDelete(const int num_rangesrvs, TransportBDeleteMessage **bdm_list) {
    TransportMessage **messages = convert_to_base(num_rangesrvs, bdm_list);
    TransportBRecvMessage *ret = return_brm(num_rangesrvs, messages);
    delete [] messages;
    return ret;
}

/**
 * return_brm
 *
 * @param num_rangesrvs the number of range servers there are
 * @param messages      an array of messages to send
 * @return a linked list of return messages
 */
TransportBRecvMessage *ThalliumEndpointGroup::return_brm(const int num_rangesrvs, TransportMessage **messages) {
    // convert the responses into a list
    TransportBRecvMessage *head = nullptr;
    TransportBRecvMessage *tail = nullptr;

    // encode each mesage
    for(int i = 0; i < num_rangesrvs; i++) {
        // pack message
        std::string sendbuf;
        if (ThalliumPacker::any(messages[i], sendbuf) != MDHIM_SUCCESS) {
            continue;
        }

        // send message and receive response
        const std::string response = rpc_->on(*endpoints_.at(messages[i]->dst))(sendbuf);

        // unpack response
        TransportRecvMessage *rm = nullptr;
        if (ThalliumUnpacker::unpack(&rm, response) != MDHIM_SUCCESS) {
            continue;
        }

        if (!rm) {
            //Skip this as the message doesn't exist
            continue;
        }

        // convert response into a TransportBRecvMessage
        TransportBRecvMessage *brm = new TransportBRecvMessage();
        brm->error = rm->error;
        brm->src = rm->src;
        brm->dst = rm->dst;
        brm->next = nullptr;
        delete rm;

        //Build the linked list to return
        if (!head) {
            head = brm;
            tail = brm;
        } else {
            tail->next = brm;
            tail = brm;
        }
    }

    return head;
}

/**
 * return_bgrm
 * This function is very similar to return_brm
 *
 * @param num_rangesrvs the number of range servers there are
 * @param messages      an array of messages to send
 * @return a linked list of return messages
 */
TransportBGetRecvMessage *ThalliumEndpointGroup::return_bgrm(const int num_rangesrvs, TransportMessage **messages) {
    // convert the responses into a list
    TransportBGetRecvMessage *head = nullptr;
    TransportBGetRecvMessage *tail = nullptr;

    for(int i = 0; i < num_rangesrvs; i++) {
        // pack message
        std::string sendbuf;
        if (ThalliumPacker::any(messages[i], sendbuf) != MDHIM_SUCCESS) {
            continue;
        }

        // send message and receive response
        const std::string response = rpc_->on(*endpoints_.at(messages[i]->dst))(sendbuf);

        // unpack response
        TransportBGetRecvMessage *bgrm = nullptr;
        if (ThalliumUnpacker::unpack(&bgrm, response) != MDHIM_SUCCESS) {
            continue;
        }

        if (!bgrm) {
            //Skip this as the message doesn't exist
            continue;
        }

        //Build the linked list to return
        bgrm->next = nullptr;
        if (!head) {
            head = bgrm;
            tail = bgrm;
        } else {
            tail->next = bgrm;
            tail = bgrm;
        }
    }

    return head;
}
