#include "ThalliumEndpointGroup.hpp"

ThalliumEndpointGroup::ThalliumEndpointGroup(mdhim_private_t *mdp)
    : TransportEndpointGroup(),
      mdp_(mdp),
      endpoints_()
{}

ThalliumEndpointGroup::~ThalliumEndpointGroup() {}

/**
 * AddID
 * Add a mapping from a unique ID to a thalllium address
 * If the engine is null or if the lookup fails, nothing
 * is done and MDHIM_ERROR is returned;
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
 * If ep is null, nothing is done and MDHIM_ERROR
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
    TransportBRecvMessage *ret = nullptr;
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
    TransportBGetRecvMessage *ret = nullptr;
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
    TransportBRecvMessage *ret = nullptr;
    delete [] messages;
    return ret;
}
