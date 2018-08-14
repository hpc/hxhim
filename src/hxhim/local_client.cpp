#include <condition_variable>
#include <cstdlib>
#include <mutex>

#include "hxhim/local_client.hpp"
#include "hxhim/private.hpp"
#include "hxhim/range_server.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

/**
 * Process a message with the range server function
 *
 * @param hx main HXHIM struct
 * @tparam msg pointer to the message
 * @treturn return_message structure with ->status = HXHIM_SUCCESS or HXHIM_ERROR
 */
template<typename Recv_t, typename Send_t>
static Recv_t *local_client(hxhim_t *hx, Send_t *msg) {
    return dynamic_cast<Recv_t *>(hxhim::range_server::range_server(hx, msg));
}

/**
 * Send put to range server
 *
 * @param hx main HXHIM struct
 * @param pm pointer to put message
 * @return return_message structure with ->status = HXHIM_SUCCESS or HXHIM_ERROR
 */
Transport::Response::Put *local_client_put(hxhim_t *hx, Transport::Request::Put *pm) {
    return local_client<Transport::Response::Put>(hx, pm);
}

/**
 * Send get to range server
 *
 * @param hx main HXHIM struct
 * @param gm pointer to get message
 * @return return_message structure with ->status = HXHIM_SUCCESS or HXHIM_ERROR
 */
Transport::Response::Get *local_client_get(hxhim_t *hx, Transport::Request::Get *gm) {
    return local_client<Transport::Response::Get>(hx, gm);
}

/**
 * Send delete to range server
 *
 * @param hx main HXHIM struct
 * @param dm pointer to delete message
 * @return return_message structure with ->status = HXHIM_SUCCESS or HXHIM_ERROR
 */
Transport::Response::Delete *local_client_delete(hxhim_t *hx, Transport::Request::Delete *dm) {
    return local_client<Transport::Response::Delete>(hx, dm);
}

/**
 * Send bulk put to range server
 *
 * @param hx main HXHIM struct
 * @param bpm pointer to bulk put message
 * @return return_message structure with ->status = HXHIM_SUCCESS or HXHIM_ERROR
*/
Transport::Response::BPut *local_client_bput(hxhim_t *hx, Transport::Request::BPut *bpm) {
    return local_client<Transport::Response::BPut>(hx, bpm);
}

/**
 * Send bulk get to range server
 *
 * @param hx main HXHIM struct
 * @param bgm pointer to get message
 * @return return_message structure with ->status = HXHIM_SUCCESS or HXHIM_ERROR
 */
Transport::Response::BGet *local_client_bget(hxhim_t *hx, Transport::Request::BGet *bgm) {
    return local_client<Transport::Response::BGet>(hx, bgm);
}

/**
 * Send get with an op and number of records greater than 1 to range server
 *
 * @param hx main HXHIM struct
 * @param gm pointer to get message
 * @return return_message structure with ->status = HXHIM_SUCCESS or HXHIM_ERROR
 */
Transport::Response::BGetOp *local_client_bget_op(hxhim_t *hx, Transport::Request::BGetOp *gm) {
    return local_client<Transport::Response::BGetOp>(hx, gm);
}

/**
 * Send bulk delete to HXHIM
 *
 * @param hx main HXHIM struct
 * @param bdm pointer to bulk delete message
 * @return return_message structure with ->status = HXHIM_SUCCESS or HXHIM_ERROR
 */
Transport::Response::BDelete *local_client_bdelete(hxhim_t *hx, Transport::Request::BDelete *bdm) {
    return local_client<Transport::Response::BDelete>(hx, bdm);
}

/**
 * Send histogram request to HXHIM
 *
 * @param hx main HXHIM struct
 * @param hist pointer to histogram message
 * @return return_message structure with ->status = HXHIM_SUCCESS or HXHIM_ERROR
 */
Transport::Response::Histogram *local_client_histogram(hxhim_t *hx, Transport::Request::Histogram *hist) {
    return local_client<Transport::Response::Histogram>(hx, hist);
}

/**
 * Send bhistogram request to HXHIM
 *
 * @param hx main HXHIM struct
 * @param bhist pointer to bhistogram message
 * @return return_message structure with ->status = HXHIM_SUCCESS or HXHIM_ERROR
 */
Transport::Response::BHistogram *local_client_bhistogram(hxhim_t *hx, Transport::Request::BHistogram *bhist) {
    return local_client<Transport::Response::BHistogram>(hx, bhist);
}
