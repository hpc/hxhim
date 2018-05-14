#include "transport.hpp"

TransportMessage::TransportMessage(const TransportMessageType type)
    : mtype(type),
      src(-1),
      dst(-1),
      index(-1),
      index_type(-1),
      index_name(nullptr)
{}

TransportMessage::~TransportMessage() {
    cleanup();
}

std::size_t TransportMessage::size() const {
    // intentional error with sizeof(char *)
    return sizeof(mtype) + sizeof(src) + sizeof(dst) + sizeof(index) + sizeof(index_type) + sizeof(char *);
}

void TransportMessage::cleanup() {
    ::operator delete(index_name);
    index_name = nullptr;
}

TransportPutMessage::TransportPutMessage()
    : TransportMessage(TransportMessageType::PUT),
      rs_idx(-1),
      key(nullptr), key_len(0), value(nullptr), value_len(0)
{}

TransportPutMessage::~TransportPutMessage() {
    cleanup();
}

std::size_t TransportPutMessage::size() const {
    return TransportMessage::size() + sizeof(rs_idx) + key_len + sizeof(key_len) + value_len + sizeof(value_len);
}

void TransportPutMessage::cleanup() {
    TransportMessage::cleanup();

    // clear the key and value
    //     if the key/value were unpacked, they have already been deleted
    //     if the key/value are local, the user owns the pointers, so they should not be deleted here
    key = nullptr;
    value = nullptr;

    key_len = 0;
    value_len = 0;
}

TransportBPutMessage::TransportBPutMessage()
    : TransportMessage(TransportMessageType::BPUT),
      rs_idx(),
      keys(), key_lens(),
      values(), value_lens(),
      num_keys(0)
{}

TransportBPutMessage::~TransportBPutMessage() {
    cleanup();
}

std::size_t TransportBPutMessage::size() const {
    std::size_t ret = TransportMessage::size() + sizeof(num_keys) + (num_keys * sizeof(decltype(rs_idx)::value_type)) + (num_keys * sizeof(decltype(key_lens)::value_type)) + (num_keys * sizeof(decltype(value_lens)::value_type));
    for(std::size_t i = 0; i < num_keys; i++) {
        ret += key_lens[i] + value_lens[i];
    }
    return ret;
}

void TransportBPutMessage::cleanup() {
    TransportMessage::cleanup();

    rs_idx.clear();

    keys.clear();
    key_lens.clear();
    values.clear();
    value_lens.clear();

    num_keys = 0;
}

TransportGet::TransportGet(const TransportMessageType type)
    : TransportMessage(type),
      op(TransportGetMessageOp::GET_OP_MAX),
      num_keys(0)
{
    // TransportMessage constructor not working right for some reason
    mtype = type;
}

TransportGet::~TransportGet() {}

TransportGetMessage::TransportGetMessage()
    : TransportGet(TransportMessageType::GET),
      rs_idx(-1),
      key(nullptr), key_len(0)

{}

TransportGetMessage::~TransportGetMessage() {
    cleanup();
}

std::size_t TransportGetMessage::size() const {
    return TransportMessage::size() + sizeof(op) + sizeof(rs_idx) + key_len + sizeof(key_len) + sizeof(num_keys);
}

void TransportGetMessage::cleanup() {
    TransportMessage::cleanup();

    key = nullptr;

    key_len = 0;

    num_keys = 0;
}

TransportBGetMessage::TransportBGetMessage()
    : TransportGet(TransportMessageType::BGET),
      rs_idx(),
      keys(), key_lens(),
      num_recs(0)
{}

TransportBGetMessage::~TransportBGetMessage() {
    cleanup();
}

std::size_t TransportBGetMessage::size() const {
    size_t ret = TransportMessage::size() + sizeof(op) + sizeof(num_keys) + (num_keys * sizeof(decltype(rs_idx)::value_type)) + sizeof(num_recs) + (num_keys * sizeof(decltype(key_lens)::value_type));
    for(std::size_t i = 0; i < num_keys; i++) {
        ret += key_lens[i];
    }

    return ret;
}

void TransportBGetMessage::cleanup() {
    TransportMessage::cleanup();

    rs_idx.clear();

    for(decltype(keys)::value_type key : keys) {
        ::operator delete(key);
    }

    keys.clear();
    key_lens.clear();

    num_keys = 0;
}

TransportDeleteMessage::TransportDeleteMessage()
    : TransportMessage(TransportMessageType::DELETE),
      rs_idx(-1),
      key(nullptr), key_len(0)
{}

TransportDeleteMessage::~TransportDeleteMessage() {
    cleanup();
}

std::size_t TransportDeleteMessage::size() const {
    return TransportMessage::size() + sizeof(rs_idx) + sizeof(key_len) + key_len;
}

void TransportDeleteMessage::cleanup() {
    TransportMessage::cleanup();

    key = nullptr;

    key_len = 0;
}

TransportBDeleteMessage::TransportBDeleteMessage()
    : TransportMessage(TransportMessageType::BDELETE),
      rs_idx(),
      keys(), key_lens(),
      num_keys(0)
{}

TransportBDeleteMessage::~TransportBDeleteMessage() {
    cleanup();
}

std::size_t TransportBDeleteMessage::size() const {
    size_t ret = TransportMessage::size() + sizeof(num_keys) + (num_keys * sizeof(decltype(rs_idx)::value_type)) + (num_keys * sizeof(decltype(key_lens)::value_type));
    for(std::size_t i = 0; i < num_keys; i++) {
        ret += key_lens[i];
    }
    return ret;
}

void TransportBDeleteMessage::cleanup() {
    TransportMessage::cleanup();

    rs_idx.clear();

    keys.clear();
    key_lens.clear();

    num_keys = 0;
}

TransportRecvMessage::TransportRecvMessage()
    : TransportMessage(TransportMessageType::RECV),
      rs_idx(-1),
      error(MDHIM_SUCCESS)
{}

TransportRecvMessage::~TransportRecvMessage() {
    cleanup();
}

std::size_t TransportRecvMessage::size() const {
    return TransportMessage::size() + sizeof(rs_idx) + sizeof(error);
}

void TransportRecvMessage::cleanup() {
    TransportMessage::cleanup();
}

TransportGetRecvMessage::TransportGetRecvMessage()
    : TransportMessage(TransportMessageType::RECV_GET),
      rs_idx(-1),
      error(MDHIM_SUCCESS),
      key(nullptr), key_len(0),
      value(nullptr), value_len(0)
{}

TransportGetRecvMessage::~TransportGetRecvMessage() {
    cleanup();
}

std::size_t TransportGetRecvMessage::size() const {
    return TransportMessage::size() + sizeof(error) + sizeof(rs_idx) + key_len + sizeof(key_len) + value_len + sizeof(value_len);
}

void TransportGetRecvMessage::cleanup() {
    TransportMessage::cleanup();

    // GetRecv deletes its key
    ::operator delete(key);
    key = nullptr;

    free(value);
    value = nullptr;

    key_len = 0;

    value_len = 0;
}

TransportBGetRecvMessage::TransportBGetRecvMessage()
    : TransportMessage(TransportMessageType::RECV_BGET),
      rs_idx(),
      error(MDHIM_SUCCESS),
      keys(), key_lens(),
      values(), value_lens(),
      num_keys(0), next(nullptr)
{}

TransportBGetRecvMessage::~TransportBGetRecvMessage() {
    cleanup();
}

std::size_t TransportBGetRecvMessage::size() const {
    size_t ret = TransportMessage::size() + sizeof(error) + sizeof(num_keys) + (num_keys * sizeof(decltype(rs_idx)::value_type)) + (num_keys * sizeof(decltype(key_lens)::value_type)) + (num_keys * sizeof(decltype(value_lens)::value_type));
    for(std::size_t i = 0; i < num_keys; i++) {
        ret += key_lens[i] + value_lens[i];
    }
    return ret;
}

void TransportBGetRecvMessage::cleanup() {
    TransportMessage::cleanup();

    rs_idx.clear();

    // BGetRecv deletes its keys
    for(decltype(keys)::value_type key : keys) {
        ::operator delete(key);
    }

    for(decltype(values)::value_type value : values) {
        free(value);
    }

    keys.clear();
    key_lens.clear();
    values.clear();
    value_lens.clear();

    // do not delete next
    next = nullptr;

    num_keys = 0;
}

TransportBRecvMessage::TransportBRecvMessage()
    : TransportMessage(TransportMessageType::RECV_BULK),
      rs_idx(),
      error(MDHIM_SUCCESS),
      num_keys(0),
      next(nullptr)
{}

TransportBRecvMessage::~TransportBRecvMessage() {
    cleanup();
}

std::size_t TransportBRecvMessage::size() const {
    return TransportMessage::size() + sizeof(rs_idx) + sizeof(num_keys) + (num_keys * sizeof(decltype(rs_idx)::value_type)) + sizeof(error);
}

void TransportBRecvMessage::cleanup() {
    TransportMessage::cleanup();

    rs_idx.clear();

    // do not delete next
    next = nullptr;
}

/**
 * get_num_srvs
 * get the number of servers that will be sent work, and need to be waited on
 *
 * @param messages      the messages that are to be sent
 * @param num_rangesrvs the total number of range servers
 * @param srvs          address of an array that will be created and filled with unique range server IDs
 * @return the          number of unique IDs in *srvs
 */
std::size_t TransportEndpointGroup::get_num_srvs(TransportMessage **messages, const std::size_t num_rangesrvs, int **srvs) {
    *srvs = nullptr;
    if (!messages || !srvs) {
        return 0;
    }

    // get the actual number of servers
    int num_srvs = 0;
    *srvs = new int[num_rangesrvs]();
    for (std::size_t i = 0; i < num_rangesrvs; i++) {
        if (!messages[i]) {
            continue;
        }

        // store server IDs to receive frome
        (*srvs)[num_srvs] = messages[i]->dst;
        num_srvs++;
    }

    if (!num_srvs) {
        delete [] *srvs;
        *srvs = nullptr;
    }

    return num_srvs;
}

Transport::Transport()
    : endpoints_(),
      endpointgroup_(nullptr)
{}

Transport::~Transport() {
    delete endpointgroup_;

    for(std::pair<const int, TransportEndpoint *> const & ep : endpoints_) {
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
void Transport::AddEndpoint(const int id, TransportEndpoint *ep) {
    endpoints_[id] = ep;
}

/**
 * RemoveEndpoint
 * Deallocates and removes the endpoint from the transport
 *
 * @param id the ID of the endpoint
 */
void Transport::RemoveEndpoint(const int id) {
    TransportEndpointMapping_t::iterator it = endpoints_.find(id);
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
void Transport::SetEndpointGroup(TransportEndpointGroup *eg) {
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
TransportRecvMessage *Transport::Put(const TransportPutMessage *pm) {
    if (!pm) {
        return nullptr;
    }
    TransportEndpointMapping_t::iterator it = endpoints_.find(pm->dst);
    return (it == endpoints_.end())?nullptr:it->second->Put(pm);
}

/**
 * Get
 * Gets a message onto the the underlying transport
 *
 * @param gm the message to GET
 * @return the response from the range server
 */
TransportGetRecvMessage *Transport::Get(const TransportGetMessage *get) {
    if (!get) {
        return nullptr;
    }
    TransportEndpointMapping_t::iterator it = endpoints_.find(get->dst);
    return (it == endpoints_.end())?nullptr:it->second->Get(get);
}

/**
 * Delete
 * Deletes a message onto the the underlying transport
 *
 * @param dm the message to DELETE
 * @return the response from the range server
 */
TransportRecvMessage *Transport::Delete(const TransportDeleteMessage *dm) {
    if (!dm) {
        return nullptr;
    }
    TransportEndpointMapping_t::iterator it = endpoints_.find(dm->dst);
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
TransportBRecvMessage *Transport::BPut(const std::size_t num_rangesrvs, TransportBPutMessage **bpm_list) {
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
TransportBGetRecvMessage *Transport::BGet(const std::size_t num_rangesrvs, TransportBGetMessage **bgm_list) {
    return (bgm_list && endpointgroup_)?endpointgroup_->BGet(num_rangesrvs, bgm_list):nullptr;
}

/**
 * BDelete
 * Bulk Delete to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list a list of DELETE messages going to different servers
 * @return the response from the range server
 */
TransportBRecvMessage *Transport::BDelete(const std::size_t num_rangesrvs, TransportBDeleteMessage **bdm_list) {
    return (bdm_list && endpointgroup_)?endpointgroup_->BDelete(num_rangesrvs, bdm_list):nullptr;
}
