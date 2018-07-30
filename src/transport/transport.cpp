#include "transport/transport.hpp"

namespace Transport {

Transport::Transport()
    : endpoints_(),
      endpointgroup_(nullptr)
{}

Transport::~Transport() {
    delete endpointgroup_;

    for(std::pair<const int, Endpoint *> const & ep : endpoints_) {
        delete ep.second;
    }
}

/**
 * AddEndpoint
 * Takes ownership of an endpoint and associates it with a unique id
 *
 * @param id the ID that the given endpoint is associated with
 * @param ep the endpoint containing the transport functionality to send and receive data
 */
void Transport::AddEndpoint(const int id, Endpoint *ep) {
    endpoints_[id] = ep;
}

/**
 * RemoveEndpoint
 * Deallocates and removes the endpoint from the transport
 *
 * @param id the ID of the endpoint
 */
void Transport::RemoveEndpoint(const int id) {
    EndpointMapping_t::iterator it = endpoints_.find(id);
    if (it != endpoints_.end()) {
        delete it->second;
        endpoints_.erase(id);
    }
}

/**
 * SetEndpointGroup
 * Takes ownership of an endpoint group, deallocating the previous one
 *
 * @param eg the endpoint group to take ownership of
 */
void Transport::SetEndpointGroup(EndpointGroup *eg) {
    if (endpointgroup_) {
        delete endpointgroup_;
    }
    endpointgroup_ = eg;
}

/**
 * Put
 * Puts a message onto the the underlying transport
 *
 * @param pm the message to PUT
 * @return the response from the range server
 */
Response::Put *Transport::Put(const Request::Put *pm) {
    if (!pm) {
        return nullptr;
    }
    EndpointMapping_t::iterator it = endpoints_.find(pm->dst);
    return (it == endpoints_.end())?nullptr:it->second->Put(pm);
}

/**
 * Get
 * Gets a message onto the the underlying transport
 *
 * @param gm the message to GET
 * @return the response from the range server
 */
Response::Get *Transport::Get(const Request::Get *get) {
    if (!get) {
        return nullptr;
    }
    EndpointMapping_t::iterator it = endpoints_.find(get->dst);
    return (it == endpoints_.end())?nullptr:it->second->Get(get);
}

/**
 * Delete
 * Deletes a message onto the the underlying transport
 *
 * @param dm the message to DELETE
 * @return the response from the range server
 */
Response::Delete *Transport::Delete(const Request::Delete *dm) {
    if (!dm) {
        return nullptr;
    }
    EndpointMapping_t::iterator it = endpoints_.find(dm->dst);
    return (it == endpoints_.end())?nullptr:it->second->Delete(dm);
}

/**
 * BPut
 * Bulk Put to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bpm_list a list of PUT messages going to different servers
 * @return the response from the range server
 */
Response::BPut *Transport::BPut(const std::size_t num_rangesrvs, Request::BPut **bpm_list) {
    return (bpm_list && endpointgroup_)?endpointgroup_->BPut(num_rangesrvs, bpm_list):nullptr;
}

/**
 * BGet
 * Bulk Get to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list a list of GET messages going to different servers
 * @return the response from the range server
 */
Response::BGet *Transport::BGet(const std::size_t num_rangesrvs, Request::BGet **bgm_list) {
    return (bgm_list && endpointgroup_)?endpointgroup_->BGet(num_rangesrvs, bgm_list):nullptr;
}

/**
 * BGetOp
 * Bulk Get to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list a list of GET messages going to different servers
 * @return the response from the range server
 */
Response::BGetOp *Transport::BGetOp(const std::size_t num_rangesrvs, Request::BGetOp **bgm_list) {
    return (bgm_list && endpointgroup_)?endpointgroup_->BGetOp(num_rangesrvs, bgm_list):nullptr;
}

/**
 * BDelete
 * Bulk Delete to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list a list of DELETE messages going to different servers
 * @return the response from the range server
 */
Response::BDelete *Transport::BDelete(const std::size_t num_rangesrvs, Request::BDelete **bdm_list) {
    return (bdm_list && endpointgroup_)?endpointgroup_->BDelete(num_rangesrvs, bdm_list):nullptr;
}

}
