//
// Created by bws on 8/24/17.
//

#ifndef MDHIM_TRANSPORT_HPP
#define MDHIM_TRANSPORT_HPP

#include <cstdlib>
#include <map>
#include <type_traits>

#include "mdhim_constants.h"
#include "transport_constants.h"

/**
 * TransportMessage
 * Specific message types are meant to inherit from this struct.
 * Utility functions should be implemented as standalone functions
 * rather than new functions in subclasses. All children of
 * TransportMessage are marked final to prevent inheritance.
 */
struct TransportMessage {
    TransportMessage(const TransportMessageType type = TransportMessageType::INVALID);
    virtual ~TransportMessage();

    virtual std::size_t size() const;
    virtual void cleanup();

    TransportMessageType mtype;
    int src;      // rank
    int dst;      // rank
    int index;
    int index_type;
    char *index_name;

    bool clean;   // whether or not this message should deallocate its pointers upon destruction
};

/**
 * TransportRequestMessage
 * This struct is just meant to provide a more specific relationship
 * between request message types than TransportMessage.
 */
struct TransportRequestMessage : public TransportMessage {
    TransportRequestMessage(const TransportMessageType type = TransportMessageType::INVALID);
    virtual ~TransportRequestMessage();
};

/**
 * TransportPutMessage
 * Put message implementation
 */
struct TransportPutMessage final : public TransportRequestMessage {
    TransportPutMessage();
    ~TransportPutMessage();

    std::size_t size() const;
    void cleanup();

    int rs_idx;

    void *key;
    std::size_t key_len;

    void *value;
    std::size_t value_len;
};

/**
 * TransportBPutMessage
 * Bulk put message imeplementation
 */
struct TransportBPutMessage final : public TransportRequestMessage {
    TransportBPutMessage();
    ~TransportBPutMessage();

    std::size_t size() const;
    void cleanup();

    int *rs_idx;

    void **keys;
    std::size_t *key_lens;

    void **values;
    std::size_t *value_lens;

    std::size_t num_keys;
};

/**
 * TransportGet
 * Base type for get messages
 */
struct TransportGet : public TransportRequestMessage {
    TransportGet(const TransportMessageType type);
    virtual ~TransportGet();

    TransportGetMessageOp op;
    std::size_t num_keys;
};

/**
 * TransportGetMessage
 * Get message implementation
 */
struct TransportGetMessage final : public TransportGet {
    TransportGetMessage();
    ~TransportGetMessage();

    std::size_t size() const;
    void cleanup();

    int rs_idx;

    void *key;
    std::size_t key_len;
};

/**
 * TransportBGetMessage
 * Bulk get message implementation
 */
struct TransportBGetMessage final : public TransportGet {
    TransportBGetMessage();
    ~TransportBGetMessage();

    std::size_t size() const;
    void cleanup();

    int *rs_idx;

    void **keys;
    std::size_t *key_lens;

    //Number of records to retrieve per key given
    std::size_t num_recs;
};

/**
 * TransportDeleteMessage
 * Delete message implementation
 */
struct TransportDeleteMessage final : public TransportRequestMessage {
    TransportDeleteMessage();
    ~TransportDeleteMessage();

    std::size_t size() const;
    void cleanup();

    int rs_idx;

    void *key;
    std::size_t key_len;
};

/**
 * TransportBDeleteMessage
 * Bulk delete message implementation
 */
struct TransportBDeleteMessage final : public TransportRequestMessage {
    TransportBDeleteMessage();
    ~TransportBDeleteMessage();

    std::size_t size() const;
    void cleanup();

    int *rs_idx;

    void **keys;
    std::size_t *key_lens;

    std::size_t num_keys;
};

/**
 * TransportResponseMessage
 * This struct is just meant to provide a more specific relationship
 * between response message types than TransportMessage.
 */
struct TransportResponseMessage : public TransportMessage {
    TransportResponseMessage(const TransportMessageType type = TransportMessageType::INVALID);
    virtual ~TransportResponseMessage();
};

/**
 * TransportRecvMessage
 * Generic receive message implementation
 */
struct TransportRecvMessage final : public TransportResponseMessage {
    TransportRecvMessage();
    ~TransportRecvMessage();

    std::size_t size() const;
    void cleanup();

    int rs_idx;

    int error;
};

/**
 * TransportGetRecvMessage
 * Generic receive get message implementation
 */
struct TransportGetRecvMessage final : public TransportResponseMessage {
    TransportGetRecvMessage();
    ~TransportGetRecvMessage();

    std::size_t size() const;
    void cleanup();

    int rs_idx;

    int error;

    void *key;
    std::size_t key_len;

    void *value;
    std::size_t value_len;
};

/**
 * TransportBGetRecvMessage
 * Bulk get receive message implementation
 */
struct TransportBGetRecvMessage final : public TransportResponseMessage {
    TransportBGetRecvMessage();
    ~TransportBGetRecvMessage();

    std::size_t size() const;
    void cleanup();

    int *rs_idx;

    int error;

    void **keys;
    std::size_t *key_lens;

    void **values;
    std::size_t *value_lens;

    std::size_t num_keys;

    TransportBGetRecvMessage *next;
};

/**
 * TransportBRecvMessage
 * Bulk receive message implementation
 */
struct TransportBRecvMessage final : public TransportResponseMessage {
    TransportBRecvMessage();
    ~TransportBRecvMessage();

    std::size_t size() const;
    void cleanup();

    int *rs_idx;

    int error;

    std::size_t num_keys;

    TransportBRecvMessage *next;
};

/**
 * @ An abstract communication endpoint
 */
class TransportEndpoint {
    public:
        virtual ~TransportEndpoint() {}

        /** @description Send a Put to this endpoint */
        virtual TransportRecvMessage *Put(const TransportPutMessage *message) = 0;

        /** @description Send a Get to this endpoint */
        virtual TransportGetRecvMessage *Get(const TransportGetMessage *message) = 0;

        /** @description Send a Put to this endpoint */
        virtual TransportRecvMessage *Delete(const TransportDeleteMessage *message) = 0;

    protected:
        TransportEndpoint() {}
        TransportEndpoint(const TransportEndpoint&  rhs) = delete;
        TransportEndpoint(const TransportEndpoint&& rhs) = delete;
};

/**
 * An abstract group of communication endpoints
 */
class TransportEndpointGroup {
    public:
        virtual ~TransportEndpointGroup() {}

        /** @description Bulk Put to multiple endpoints    */
        virtual TransportBRecvMessage *BPut(const std::size_t num_rangesrvs, TransportBPutMessage **bpm_list) = 0;

        /** @description Bulk Get from multiple endpoints  */
        virtual TransportBGetRecvMessage *BGet(const std::size_t num_rangesrvs, TransportBGetMessage **bgm_list) = 0;

        /** @description Bulk Delete to multiple endpoints */
        virtual TransportBRecvMessage *BDelete(const std::size_t num_rangesrvs, TransportBDeleteMessage **bdm_list) = 0;

   protected:
        TransportEndpointGroup() {}
        TransportEndpointGroup(const TransportEndpointGroup&  rhs) = delete;
        TransportEndpointGroup(const TransportEndpointGroup&& rhs) = delete;

        /** @description Converts Transport*Message ** to TransportMessage ** to prevent pointer weirdness */
        template <typename T, typename = std::enable_if_t<std::is_convertible<T, TransportMessage>::value> >
        static TransportMessage **convert_to_base(const std::size_t num_rangesrvs, T **list) {
            if (!list) {
                return nullptr;
            }

            TransportMessage **messages = new TransportMessage *[num_rangesrvs]();
            for(std::size_t i = 0; i < num_rangesrvs; i++) {
                messages[i] = list[i];
            }

            return messages;
        }

        /** @description Counts and returns the servers that will be sent work */
        std::size_t get_num_srvs(TransportMessage **messages, const std::size_t num_rangesrvs, int **srvs);
};

/**
 * Transport interface for endpoints
 * The endpoints and endpoint group do not have to
 * use the same underlying transport protocols.
 *
 * Transport takes ownership of the EP and EG pointers
 */
class Transport {
    public:
        Transport();
        ~Transport();

        /** @description Takes ownership of an endpoint and associates it with a unique id   */
        void AddEndpoint(const int id, TransportEndpoint *ep);

        /** @description Deallocates and removes the endpoint from the transport             */
        void RemoveEndpoint(const int id);

        /** @description Takes ownership of an endpoint group, deallocating the previous one */
        void SetEndpointGroup(TransportEndpointGroup *eg);

        /**  @description Puts a message onto the the underlying transport    */
        TransportRecvMessage *Put(const TransportPutMessage *pm);

        /**  @description Gets a message onto the the underlying transport    */
        TransportGetRecvMessage *Get(const TransportGetMessage *gm);

        /**  @description Deletes a message onto the the underlying transport */
        TransportRecvMessage *Delete(const TransportDeleteMessage *dm);

        /** @description Bulk Put to multiple endpoints     */
        TransportBRecvMessage *BPut(const std::size_t num_rangesrvs, TransportBPutMessage **bpm_list);

        /** @description Bulk Get from multiple endpoints   */
        TransportBGetRecvMessage *BGet(const std::size_t num_rangesrvs, TransportBGetMessage **bgm_list);

        /** @description Bulk Delete to multiple endpoints  */
        TransportBRecvMessage *BDelete(const std::size_t num_rangesrvs, TransportBDeleteMessage **bdm_list);

    private:
        typedef std::map<int, TransportEndpoint *> TransportEndpointMapping_t;

        TransportEndpointMapping_t endpoints_;
        TransportEndpointGroup *endpointgroup_;
};

#endif //HXHIM_TRANSPORT
