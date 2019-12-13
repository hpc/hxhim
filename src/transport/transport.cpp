#include <cmath>

#include "transport/transport.hpp"

namespace Transport {

EndpointGroup::EndpointGroup() {}

EndpointGroup::~EndpointGroup() {}

Response::RecvBPut *EndpointGroup::communicate(const std::unordered_map<int, Request::SendBPut *> &) {
    return nullptr;
}

Response::RecvBGet *EndpointGroup::communicate(const std::unordered_map<int, Request::SendBGet *> &) {
    return nullptr;
}

Response::RecvBGet2 *EndpointGroup::communicate(const std::unordered_map<int, Request::SendBGet2 *> &) {
    return nullptr;
}

Response::RecvBGetOp *EndpointGroup::communicate(const std::unordered_map<int, Request::SendBGetOp *> &) {
    return nullptr;
}

Response::RecvBDelete *EndpointGroup::communicate(const std::unordered_map<int, Request::SendBDelete *> &) {
    return nullptr;
}

Response::RecvBHistogram *EndpointGroup::communicate(const std::unordered_map<int, Request::SendBHistogram *> &) {
    return nullptr;
}

Transport::Transport()
    : endpointgroup_(nullptr)
{}

Transport::~Transport() {
    for(decltype(endpoints_)::value_type const & ep : endpoints_) {
        delete ep.second;
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
 * BPut
 * Bulk Put to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bpm_list a list of PUT messages going to different servers
 * @return the response from the range server
 */
Response::RecvBPut *Transport::communicate(const std::unordered_map<int, Request::SendBPut *> &bpm_list) {
    return (bpm_list.size() && endpointgroup_)?endpointgroup_->communicate(bpm_list):nullptr;
}

/**
 * BGet
 * Bulk Get to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list a list of GET messages going to different servers
 * @return the response from the range server
 */
Response::RecvBGet *Transport::communicate(const std::unordered_map<int, Request::SendBGet *> &bgm_list) {
    return (bgm_list.size() && endpointgroup_)?endpointgroup_->communicate(bgm_list):nullptr;
}

/**
 * BGet2
 * Bulk Get2 to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list a list of GET messages going to different servers
 * @return the response from the range server
 */
Response::RecvBGet2 *Transport::communicate(const std::unordered_map<int, Request::SendBGet2 *> &bgm_list) {
    return (bgm_list.size() && endpointgroup_)?endpointgroup_->communicate(bgm_list):nullptr;
}

/**
 * BGetOp
 * Bulk Get to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list a list of GET messages going to different servers
 * @return the response from the range server
 */
Response::RecvBGetOp *Transport::communicate(const std::unordered_map<int, Request::SendBGetOp *> &bgm_list) {
    return (bgm_list.size() && endpointgroup_)?endpointgroup_->communicate(bgm_list):nullptr;
}

/**
 * BDelete
 * Bulk Delete to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list a list of DELETE messages going to different servers
 * @return the response from the range server
 */
Response::RecvBDelete *Transport::communicate(const std::unordered_map<int, Request::SendBDelete *> &bdm_list) {
    return (bdm_list.size() && endpointgroup_)?endpointgroup_->communicate(bdm_list):nullptr;
}

/**
 * BHistogram
 * Bulk Histogram to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list a list of HISTOGRAM messages going to different servers
 * @return the response from the range server
 */
Response::RecvBHistogram *Transport::communicate(const std::unordered_map<int, Request::SendBHistogram *> &bhm_list) {
    return (bhm_list.size() && endpointgroup_)?endpointgroup_->communicate(bhm_list):nullptr;
}

}
