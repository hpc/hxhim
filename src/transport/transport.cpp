#include <cmath>

#include "transport/transport.hpp"

namespace Transport {

Endpoint::Endpoint() {}

Endpoint::~Endpoint() {}

Response::Put *Endpoint::Put(const Request::Put *) {
    return nullptr;
}

Response::Get *Endpoint::Get(const Request::Get *) {
    return nullptr;
}

Response::Delete *Endpoint::Delete(const Request::Delete *) {
    return nullptr;
}

Response::Histogram *Endpoint::Histogram(const Request::Histogram *) {
    return nullptr;
}

EndpointGroup::EndpointGroup() {}

EndpointGroup::~EndpointGroup() {}

Response::BPut *EndpointGroup::BPut(const std::map<int, Request::BPut *> &) {
    return nullptr;
}

Response::BGet *EndpointGroup::BGet(const std::map<int, Request::BGet *> &) {
    return nullptr;
}

Response::BGetOp *EndpointGroup::BGetOp(const std::map<int, Request::BGetOp *> &) {
    return nullptr;
}

Response::BDelete *EndpointGroup::BDelete(const std::map<int, Request::BDelete *> &) {
    return nullptr;
}

Response::BHistogram *EndpointGroup::BHistogram(const std::map<int, Request::BHistogram *> &) {
    return nullptr;
}

Transport::Transport()
    : endpoints_(),
      endpointgroup_(nullptr)
{}

Transport::~Transport() {
    SetEndpointGroup(nullptr);

    for(decltype(endpoints_)::value_type const & ep : endpoints_) {
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
 * Histogram
 * Sends a histogram message onto the the underlying transport
 *
 * @param hm the HISTOGRAM message
 * @return the response from the range server
 */
Response::Histogram *Transport::Histogram(const Request::Histogram *hm) {
    if (!hm) {
        return nullptr;
    }
    EndpointMapping_t::iterator it = endpoints_.find(hm->dst);
    return (it == endpoints_.end())?nullptr:it->second->Histogram(hm);
}

/**
 * BPut
 * Bulk Put to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bpm_list a list of PUT messages going to different servers
 * @return the response from the range server
 */
Response::BPut *Transport::BPut(const std::map<int, Request::BPut *> &bpm_list) {
    return (bpm_list.size() && endpointgroup_)?endpointgroup_->BPut(bpm_list):nullptr;
}

/**
 * BGet
 * Bulk Get to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list a list of GET messages going to different servers
 * @return the response from the range server
 */
Response::BGet *Transport::BGet(const std::map<int, Request::BGet *> &bgm_list) {
    return (bgm_list.size() && endpointgroup_)?endpointgroup_->BGet(bgm_list):nullptr;
}

/**
 * BGetOp
 * Bulk Get to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list a list of GET messages going to different servers
 * @return the response from the range server
 */
Response::BGetOp *Transport::BGetOp(const std::map<int, Request::BGetOp *> &bgm_list) {
    return (bgm_list.size() && endpointgroup_)?endpointgroup_->BGetOp(bgm_list):nullptr;
}

/**
 * BDelete
 * Bulk Delete to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list a list of DELETE messages going to different servers
 * @return the response from the range server
 */
Response::BDelete *Transport::BDelete(const std::map<int, Request::BDelete *> &bdm_list) {
    return (bdm_list.size() && endpointgroup_)?endpointgroup_->BDelete(bdm_list):nullptr;
}

/**
 * BHistogram
 * Bulk Histogram to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list a list of HISTOGRAM messages going to different servers
 * @return the response from the range server
 */
Response::BHistogram *Transport::BHistogram(const std::map<int, Request::BHistogram *> &bhm_list) {
    return (bhm_list.size() && endpointgroup_)?endpointgroup_->BHistogram(bhm_list):nullptr;
}

}
