#include <cmath>

#include "transport/transport.hpp"

Transport::EndpointGroup::EndpointGroup() {}

Transport::EndpointGroup::~EndpointGroup() {}

Message::Response::BPut *
Transport::EndpointGroup::communicate(const ReqList<Message::Request::BPut> &) {
    return nullptr;
}

Message::Response::BGet *
Transport::EndpointGroup::communicate(const ReqList<Message::Request::BGet> &) {
    return nullptr;
}

Message::Response::BGetOp *
Transport::EndpointGroup::communicate(const ReqList<Message::Request::BGetOp> &) {
    return nullptr;
}

Message::Response::BDelete *
Transport::EndpointGroup::communicate(const ReqList<Message::Request::BDelete> &) {
    return nullptr;
}

Message::Response::BHistogram *
Transport::EndpointGroup::communicate(const ReqList<Message::Request::BHistogram> &) {
    return nullptr;
}

Transport::RangeServer::~RangeServer() {}

Transport::Transport::Transport(EndpointGroup *epg, RangeServer *rs)
    : endpointgroup_(nullptr),
      rangeserver_(nullptr)
{
    SetEndpointGroup(epg);
    SetRangeServer(rs);
}

Transport::Transport::~Transport() {
    SetRangeServer(nullptr);
    SetEndpointGroup(nullptr);
}

/**
 * SetEndpointGroup
 * Takes ownership of an endpoint group, deallocating the previous one
 *
 * @param eg the endpoint group to take ownership of
 */
void Transport::Transport::SetEndpointGroup(EndpointGroup *eg) {
    destruct(endpointgroup_);
    endpointgroup_ = eg;
}

/**
 * SetRangeserver
 * Takes ownership of a range server, deallocating the previous one
 *
 * @param rs the range server to take ownership of
 */
void Transport::Transport::SetRangeServer(RangeServer *rs) {
    destruct(rangeserver_);
    rangeserver_ = rs;
}

/**
 * BPut
 * Bulk Put to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bpm_list a list of PUT messages going to different servers
 * @return the response from the range server
 */
Message::Response::BPut *
Transport::Transport::communicate(const ReqList<Message::Request::BPut> &bpm_list) {
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
Message::Response::BGet *
Transport::Transport::communicate(const ReqList<Message::Request::BGet> &bgm_list) {
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
Message::Response::BGetOp *
Transport::Transport::communicate(const ReqList<Message::Request::BGetOp> &bgm_list) {
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
Message::Response::BDelete *
Transport::Transport::communicate(const ReqList<Message::Request::BDelete> &bdm_list) {
    return (bdm_list.size() && endpointgroup_)?endpointgroup_->communicate(bdm_list):nullptr;
}

/**
 * BHistogram
 * Bulk Histogram to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bhm_list a list of HISTOGRAM messages going to different servers
 * @return the response from the range server
 */
Message::Response::BHistogram *
Transport::Transport::communicate(const ReqList<Message::Request::BHistogram> &bhm_list) {
    return (bhm_list.size() && endpointgroup_)?endpointgroup_->communicate(bhm_list):nullptr;
}
