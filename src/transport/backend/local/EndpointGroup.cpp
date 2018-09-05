#include "hxhim/local_client.hpp"
#include "hxhim/private.hpp"
#include "transport/backend/local/EndpointGroup.hpp"

namespace Transport {
namespace local {

EndpointGroup::EndpointGroup(hxhim_t *hx)
  : ::Transport::EndpointGroup(),
    hx(hx)
{}

EndpointGroup::~EndpointGroup() {}

/**
 * BPut
 *
 * @param num_rangesrvs the total number of range servers
 * @param bpm_list the list of BPUT messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::BPut *EndpointGroup::BPut(const std::size_t num_rangesrvs, Request::BPut **bpm_list) {
    return local_client_bput(hx, bpm_list[hx->p->bootstrap.rank]);
}

/**
 * BGet
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list the list of BGET messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::BGet *EndpointGroup::BGet(const std::size_t num_rangesrvs, Request::BGet **bgm_list) {
    return local_client_bget(hx, bgm_list[hx->p->bootstrap.rank]);
}

/**
 * BGetOp
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list the list of BGET messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::BGetOp *EndpointGroup::BGetOp(const std::size_t num_rangesrvs, Request::BGetOp **bgm_list) {
    return local_client_bget_op(hx, bgm_list[hx->p->bootstrap.rank]);
}

/**
 * BDelete
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list the list of BDELETE messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::BDelete *EndpointGroup::BDelete(const std::size_t num_rangesrvs, Request::BDelete **bdm_list) {
    return local_client_bdelete(hx, bdm_list[hx->p->bootstrap.rank]);
}

/**
 * BHistogram
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list the list of BDELETE messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::BHistogram *EndpointGroup::BHistogram(const std::size_t num_rangesrvs, Request::BHistogram **bhist_list) {
    return local_client_bhistogram(hx, bhist_list[hx->p->bootstrap.rank]);
}

}
}
