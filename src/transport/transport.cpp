#include <cmath>

#include "transport/transport.hpp"

namespace Transport {

EndpointGroup::EndpointGroup() {}

EndpointGroup::~EndpointGroup() {}

Response::BPut *EndpointGroup::communicate(const std::unordered_map<int, Request::BPut *> &) {
    return nullptr;
}

Response::BGet *EndpointGroup::communicate(const std::unordered_map<int, Request::BGet *> &) {
    return nullptr;
}

Response::BGetOp *EndpointGroup::communicate(const std::unordered_map<int, Request::BGetOp *> &) {
    return nullptr;
}

Response::BDelete *EndpointGroup::communicate(const std::unordered_map<int, Request::BDelete *> &) {
    return nullptr;
}

Response::BHistogram *EndpointGroup::communicate(const std::unordered_map<int, Request::BHistogram *> &) {
    return nullptr;
}

Transport::Transport()
    : endpointgroup_(nullptr)
{}

Transport::~Transport() {
    SetEndpointGroup(nullptr);
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
Response::BPut *Transport::communicate(const std::unordered_map<int, Request::BPut *> &bpm_list) {
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
Response::BGet *Transport::communicate(const std::unordered_map<int, Request::BGet *> &bgm_list) {
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
Response::BGetOp *Transport::communicate(const std::unordered_map<int, Request::BGetOp *> &bgm_list) {
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
Response::BDelete *Transport::communicate(const std::unordered_map<int, Request::BDelete *> &bdm_list) {
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
Response::BHistogram *Transport::communicate(const std::unordered_map<int, Request::BHistogram *> &bhm_list) {
    return (bhm_list.size() && endpointgroup_)?endpointgroup_->communicate(bhm_list):nullptr;
}

}
