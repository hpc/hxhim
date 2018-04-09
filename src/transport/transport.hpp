//
// Created by bws on 8/24/17.
//

#ifndef HXHIM_TRANSPORT
#define HXHIM_TRANSPORT

#include <cstdlib>
#include <map>

#include "index_struct.h"
#include "mdhim_constants.h"
#include "transport_constants.h"

/**
 * Base Message Type
 * Specific message types are meant to inherit from this class.
 * Utility functions should be implemented as standalone functions
 * rather than new functions in subclasses. All child classes of
 * TransportMessage are marked final to prevent inheritance.
 */
class TransportMessage {
    public:
        TransportMessage(const TransportMessageType type = TransportMessageType::INVALID);

        virtual ~TransportMessage();

        virtual int size() const;
        virtual void cleanup();

        TransportMessageType mtype;
        int src;
        int dst;
        int index;
        int index_type;
        char *index_name;
};

/**
 * TransportPutMessage
 * Put message implementation
 */
class TransportPutMessage final : virtual public TransportMessage {
    public:
        TransportPutMessage();
        ~TransportPutMessage();

        int size() const;
        void cleanup();

        void *key;
        int key_len;
        void *value;
        int value_len;
};

/**
 * TransportBPutMessage
 * Bulk put message imeplementation
 */
class TransportBPutMessage final : virtual public TransportMessage {
    public:
        TransportBPutMessage();
        ~TransportBPutMessage();

        int size() const;
        void cleanup();

        void **keys;
        int *key_lens;
        void **values;
        int *value_lens;
        int num_keys;
};

/**
 * TransportGet
 * Base type for get messages
 */
class TransportGet : virtual public TransportMessage {
    public:
        TransportGet(const TransportMessageType type);
        virtual ~TransportGet();

        TransportGetMessageOp op;
        int num_keys;
};

/**
 * TransportGetMessage
 * Get message implementation
 */
class TransportGetMessage final : virtual public TransportGet {
    public:
        TransportGetMessage();
        ~TransportGetMessage();

        int size() const;
        void cleanup();

        void *key;
        int key_len;
};

/**
 * TransportBGetMessage
 * Bulk get message implementation
 */
class TransportBGetMessage final : virtual public TransportGet {
    public:
        TransportBGetMessage();
        ~TransportBGetMessage();

        int size() const;
        void cleanup();

        void **keys;
        int *key_lens;

        //Number of records to retrieve per key given
        int num_recs;
};

/**
 * TransportDeleteMessage
 * Delete message implementation
 */
class TransportDeleteMessage final : virtual public TransportMessage {
    public:
        TransportDeleteMessage();
        ~TransportDeleteMessage();

        int size() const;
        void cleanup();

        void *key;
        int key_len;
};

/**
 * TransportBDeleteMessage
 * Bulk delete message implementation
 */
class TransportBDeleteMessage final : virtual public TransportMessage {
    public:
        TransportBDeleteMessage();
        ~TransportBDeleteMessage();

        int size() const;
        void cleanup();

        void **keys;
        int *key_lens;
        int num_keys;
};

/**
 * TransportRecvMessage
 * Generic receive message implementation
 */
class TransportRecvMessage final : virtual public TransportMessage {
    public:
        TransportRecvMessage();
        ~TransportRecvMessage();

        int size() const;
        void cleanup();

        int error;
};

/**
 * TransportGetRecvMessage
 * Generic receive get message implementation
 */
class TransportGetRecvMessage final : virtual public TransportMessage {
    public:
        TransportGetRecvMessage();
        ~TransportGetRecvMessage();

        int size() const;
        void cleanup();

        int error;
        void *key;
        int key_len;
        void *value;
        int value_len;
};

/**
 * TransportBGetRecvMessage
 * Bulk get receive message implementation
 */
class TransportBGetRecvMessage final : virtual public TransportMessage {
    public:
        TransportBGetRecvMessage();
        ~TransportBGetRecvMessage();

        int size() const;
        void cleanup();

        int error;
        void **keys;
        int *key_lens;
        void **values;
        int *value_lens;
        int num_keys;
        TransportBGetRecvMessage *next;
};

/**
 * TransportBRecvMessage
 * Bulk receive message implementation
 */
class TransportBRecvMessage final : virtual public TransportMessage {
    public:
        TransportBRecvMessage();
        ~TransportBRecvMessage();

        int size() const;
        void cleanup();

        int error;
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

    protected:
        TransportEndpoint() {}

        TransportEndpoint(const TransportEndpoint& rhs) {}
};

typedef std::map<int, TransportEndpoint *> TransportEndpointMapping_t;

/**
 * An abstract group of communication endpoints
 */
class TransportEndpointGroup {
    public:
        TransportEndpointGroup();
        virtual ~TransportEndpointGroup();

        /** @description Takes ownership of an endpoint and associates it with a unique id */
        void AddEndpoint(const int id, TransportEndpoint *ep);

        /** @description Deallocates and removes the endpoint from the transport */
        void RemoveEndpoint(const int id);

        /** @description Bulk Put to multiple endpoints   */
        virtual TransportBRecvMessage *BPut(const int num_rangesrvs, TransportBPutMessage **bpm_list) = 0;

        /** @description Bulk Get from multiple endpoints */
        virtual TransportBGetRecvMessage *BGet(const int num_rangesrvs, TransportBGetMessage **bgm_list) = 0;

        /** @description Bulk Delete to multiple endpoints   */
        virtual TransportBRecvMessage *BDelete(const int num_rangesrvs, TransportBDeleteMessage **bpm_list) = 0;

   protected:
        TransportEndpointGroup(const TransportEndpointGroup&  rhs) = delete;
        TransportEndpointGroup(const TransportEndpointGroup&& rhs) = delete;

        TransportEndpointMapping_t endpoints_;
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

        /** @description Takes ownership of an endpoint and associates it with a unique id */
        void AddEndpoint(const int id, TransportEndpoint *ep);

        /** @description Deallocates and removes the endpoint from the transport */
        void RemoveEndpoint(const int id);

        /** @description Takes ownership of an endpoint group, deallocating the previous one */
        void SetEndpointGroup(TransportEndpointGroup *eg);

        /**  @description PUTs a message onto the the underlying transport */
        TransportRecvMessage *Put(const TransportPutMessage *put);

        /**  @description GETs a message onto the the underlying transport */
        TransportGetRecvMessage *Get(const TransportGetMessage *get);

        /** @description Bulk Put to multiple endpoints  */
        TransportBRecvMessage *BPut(const int num_rangesrvs, TransportBPutMessage **bpm_list);

        /** @description Bulk Get from multiple endpoints  */
        TransportBGetRecvMessage *BGet(const int num_rangesrvs, TransportBGetMessage **bgm_list);

        /** @description Bulk Delete to multiple endpoints  */
        TransportBRecvMessage *BDelete(const int num_rangesrvs, TransportBDeleteMessage **bdm_list);

    private:
        TransportEndpointMapping_t endpoints_;
        TransportEndpointGroup *endpointgroup_;
};

#endif //HXHIM_TRANSPORT
