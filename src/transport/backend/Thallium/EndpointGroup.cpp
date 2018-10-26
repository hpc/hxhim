#if HXHIM_HAVE_THALLIUM

#include "transport/backend/Thallium/EndpointGroup.hpp"

Transport::Thallium::EndpointGroup::EndpointGroup(const Thallium::Engine_t &engine,
                                                  const Thallium::RPC_t &rpc,
                                                  FixedBufferPool *packed,
                                                  FixedBufferPool *responses,
                                                  FixedBufferPool *arrays,
                                                  FixedBufferPool *buffers)
  : ::Transport::EndpointGroup(),
    engine(engine),
    rpc(rpc),
    endpoints(),
    packed(packed),
    responses(responses),
    arrays(arrays),
    buffers(buffers)
{
    if (!rpc) {
        throw std::runtime_error("thallium::remote_procedure in Thallium Endpoint Group must not be nullptr");
    }

    if (!packed) {
        throw std::runtime_error("FixedBufferPool in Thallium Endpoint Group must not be nullptr");
    }

    if (!responses) {
        throw std::runtime_error("FixedBufferPool in Thallium Endpoint Group must not be nullptr");
    }

    if (!arrays) {
        throw std::runtime_error("FixedBufferPool in Thallium Endpoint Group must not be nullptr");
    }

    if (!buffers) {
        throw std::runtime_error("FixedBufferPool in Thallium Endpoint Group must not be nullptr");
    }

}

Transport::Thallium::EndpointGroup::~EndpointGroup() {}

/**
 * AddID
 * Add a mapping from a unique ID to a thalllium address
 * ThalliumEndpointGroup takes ownership of ep
 *     - If the engine is null or if the lookup fails,
 *       nothing is done and TRANSPORT_ERROR is returned
 *
 * @param id       the unique ID associated with the rank
 * @param engine   the engine instance
 * @param address  the address of the other end
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR
 */
int Transport::Thallium::EndpointGroup::AddID(const int id, const Thallium::Engine_t &engine, const std::string &address) {
    if (!engine) {
        return TRANSPORT_ERROR;
    }

    Thallium::Endpoint_t ep(new thallium::endpoint(engine->lookup(address)));
    if (!ep) {
        return TRANSPORT_ERROR;
    }

    return AddID(id, ep);
}

/**
 * AddID
 * Add a mapping from a unique ID to a MPI rank
 * ThalliumEndpointGroup takes ownership of ep
 *     - If ep is null, nothing is done
 *       and TRANSPORT_ERROR is returned
 *     - If there is already an endpoint at id,
 *       the endpoint is destroyed
 *
 * @param id the unique ID associated with the rank
 * @param ep the other end
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR
 */
int Transport::Thallium::EndpointGroup::AddID(const int id, const Thallium::Endpoint_t &ep) {
    if (!ep) {
        return TRANSPORT_ERROR;
    }

    endpoints[id] = ep;
    return TRANSPORT_SUCCESS;
}

/**
 * RemoveID
 * Remove a mapping from a unique ID to a rank
 * If the unique ID is not found, nothing is done
 *
 * @param id the unique ID to remove
 */
void Transport::Thallium::EndpointGroup::RemoveID(const int id) {
    endpoints.erase(id);
}

/**
 * BPut
 *
 * @param num_rangesrvs the total number of range servers
 * @param bpm_list the list of BPUT messages to send
 * @return a linked list of response messages, or nullptr
 */
Transport::Response::BPut *Transport::Thallium::EndpointGroup::BPut(const std::map<int, Request::BPut *> &bpm_list) {
    return do_operation<Response::BPut>(bpm_list);
}

/**
 * BGet
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list the list of BGET messages to send
 * @return a linked list of response messages, or nullptr
 */
Transport::Response::BGet *Transport::Thallium::EndpointGroup::BGet(const std::map<int, Request::BGet *> &bgm_list) {
    return do_operation<Response::BGet>(bgm_list);
}

/**
 * BGetOp
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list the list of BGETOP messages to send
 * @return a linked list of response messages, or nullptr
 */
Transport::Response::BGetOp *Transport::Thallium::EndpointGroup::BGetOp(const std::map<int, Request::BGetOp *> &bgm_list) {
    return do_operation<Response::BGetOp>(bgm_list);
}

/**
 * BDelete
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list the list of BDELETE messages to send
 * @return a linked list of response messages, or nullptr
 */
Transport::Response::BDelete *Transport::Thallium::EndpointGroup::BDelete(const std::map<int, Request::BDelete *> &bdm_list) {
    return do_operation<Response::BDelete>(bdm_list);
}

/**
 * BHistogram
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list the list of BDELETE messages to send
 * @return a linked list of response messages, or nullptr
 */
Transport::Response::BHistogram *Transport::Thallium::EndpointGroup::BHistogram(const std::map<int, Request::BHistogram *> &bhist_list) {
    return do_operation<Response::BHistogram>(bhist_list);
}

#endif
