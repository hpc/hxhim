#include <cmath>

#include "transport/backend/MPI/EndpointGroup.hpp"
#include "utils/FixedBufferPool.hpp"

namespace Transport {
namespace MPI {

EndpointGroup::EndpointGroup(const MPI_Comm comm,
                             volatile std::atomic_bool &running,
                             FixedBufferPool *packed,
                             FixedBufferPool *responses,
                             FixedBufferPool *arrays,
                             FixedBufferPool *buffers)
  : ::Transport::EndpointGroup(),
    EndpointBase(comm, packed),
    ranks(),
    running(running),
    mpi_requests(new FixedBufferPool(sizeof(MPI_Request), std::max((size - 1) * 2, 1), "MPI_Request")),
    ptrs(new FixedBufferPool(sizeof(void *) * size, 3, "MPI Pointers")),
    lens(new std::size_t[size]),
    dsts(new int[size]),
    srvs(new int[size]),
    responses(responses),
    arrays(arrays),
    buffers(buffers)
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
Response::BPut *EndpointGroup::communicate(const std::unordered_map<int, Request::BPut *> &bpm_list) {
    return return_msgs<Response::BPut>(bpm_list);
}

/**
 * BGet
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list the list of BGET messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::BGet *EndpointGroup::communicate(const std::unordered_map<int, Request::BGet *> &bgm_list) {
    return return_msgs<Response::BGet>(bgm_list);
}

/**
 * BGetOp
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list the list of BGET messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::BGetOp *EndpointGroup::communicate(const std::unordered_map<int, Request::BGetOp *> &bgm_list) {
    return return_msgs<Response::BGetOp>(bgm_list);
}

/**
 * BDelete
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list the list of BDELETE messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::BDelete *EndpointGroup::communicate(const std::unordered_map<int, Request::BDelete *> &bdm_list) {
    return return_msgs<Response::BDelete>(bdm_list);
}

/**
 * BHistogram
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list the list of BDELETE messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::BHistogram *EndpointGroup::communicate(const std::unordered_map<int, Request::BHistogram *> &bhist_list) {
    return return_msgs<Response::BHistogram>(bhist_list);
}

}
}
