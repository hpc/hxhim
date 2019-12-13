#include <cmath>

#include "transport/MPI/EndpointGroup.hpp"
#include "utils/memory.hpp"

namespace Transport {
namespace MPI {

EndpointGroup::EndpointGroup(const MPI_Comm comm,
                             volatile std::atomic_bool &running)
  : ::Transport::EndpointGroup(),
    EndpointBase(comm),
    ranks(),
    running(running),
    lens(new std::size_t[size]),
    dsts(new int[size]),
    srvs(new int[size])
{}

EndpointGroup::~EndpointGroup() {
    delete [] srvs;
    delete [] dsts;
    delete [] lens;
}

/**
 * AddID
 * Add a mapping from a unique ID to a MPI rank
 *
 * @param id the unique ID associated with the rank
 * @param rank the rank associated with the unique ID
 */
void EndpointGroup::AddID(const int id, const int rank) {
    ranks[id] = rank;
}

/**
 * RemoveID
 * Remove a mapping from a unique ID to a rank
 * If the unique ID is not found, nothing is done
 *
 * @param id the unique ID to remove
 */
void EndpointGroup::RemoveID(const int id) {
    ranks.erase(id);
}

/**
 * BPut
 *
 * @param num_rangesrvs the total number of range servers
 * @param bpm_list the list of BPUT messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::RecvBPut *EndpointGroup::communicate(const std::unordered_map<int, Request::SendBPut *> &bpm_list) {
    return return_msgs<Response::RecvBPut>(bpm_list);
}

/**
 * BGet
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list the list of BGET messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::RecvBGet *EndpointGroup::communicate(const std::unordered_map<int, Request::SendBGet *> &bgm_list) {
    return return_msgs<Response::RecvBGet>(bgm_list);
}

/**
 * BGetOp
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list the list of BGET messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::RecvBGetOp *EndpointGroup::communicate(const std::unordered_map<int, Request::SendBGetOp *> &bgm_list) {
    return return_msgs<Response::RecvBGetOp>(bgm_list);
}

/**
 * BDelete
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list the list of BDELETE messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::RecvBDelete *EndpointGroup::communicate(const std::unordered_map<int, Request::SendBDelete *> &bdm_list) {
    return return_msgs<Response::RecvBDelete>(bdm_list);
}

// /**
//  * BHistogram
//  *
//  * @param num_rangesrvs the total number of range servers
//  * @param bdm_list the list of BDELETE messages to send
//  * @return a linked list of response messages, or nullptr
//  */
// Response::RecvBHistogram *EndpointGroup::communicate(const std::unordered_map<int, Request::SendBHistogram *> &bhist_list) {
//     return return_msgs<Response::RecvBHistogram>(bhist_list);
// }

}
}
