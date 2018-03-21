//
// Created by bws on 8/24/17.
//

#ifndef HXHIM_TRANSPORT
#define HXHIM_TRANSPORT

#include <cstdlib>
#include <map>

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

        int size() const;
        void cleanup();

        TransportMessageType mtype;
        int src;
        int dst;
        int index;
        int index_type;
        char *index_name;

    protected:
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
        TransportGet();
        ~TransportGet();

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
 * Generic receive message implemenetation
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
 * Generic receive get message implemenetation
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
 *
 * This is a stateful API that allows the user to enqueue PUT and GET operations,
 * and use the flush call to complete progress. You initiate a Put or Get and receive an operation id. You then
 * keep calling the function until the operation is completed.
 *
 * TODO: replace all message structs with pointer to base message type
 */
class TransportEndpoint {
    public:
        virtual ~TransportEndpoint() {}

        /** @description Enqueue a Put request for this endpoint */
        virtual int AddPutRequest(const TransportPutMessage *message) = 0;

        /** @description Enqueue a Get request for this endpoint */
        virtual int AddGetRequest(const TransportGetMessage *message) = 0;

        /** @description Enqueue a Put reply for the request originator */
        virtual int AddPutReply(TransportRecvMessage **message) = 0;

        /** @description Enqueue a Get reply for the request originator */
        virtual int AddGetReply(TransportGetRecvMessage **message) = 0;

    protected:
        TransportEndpoint() {}

        TransportEndpoint(const TransportEndpoint& rhs) {}
};

// /**
//  * An abstract group of communication endpoints
//  * @param Transport
//  */
// class TransportEndpointGroup {
//     public:
//         virtual ~TransportEndpointGroup() {}

//         /** @description Enqueue a BPut requests to multiple endpoints  */
//         virtual int AddBPutRequest(TransportBPutMessage **messages, int num_srvs) = 0;

//         /** @description Enqueue a BGet request to multiple endpoints  */
//         virtual int AddBGetRequest(TransportBGetMessage **messages, int num_srvs) = 0;

//         /** @description Enqueue a BPut reply for the request originator */
//         virtual int AddBPutReply(TransportRecvMessage **message) = 0;

//         /** @description Enqueue a BGet reply for the request originator */
//         virtual int AddBGetReply(TransportBGetRecvMessage **messages) = 0;

//    protected:
//         TransportEndpointGroup() {}

//         TransportEndpointGroup(const TransportEndpointGroup& rhs){}
// };

/**
 * Abstract base for all Transport Implementations (MPI, Sockets, Mercury, etc.)
 * Transport takes ownership of the EP and EG pointers
 */
class Transport {
    public:
        Transport(const int &id)
            : endpoints_(),
              id_(id)
        {}

        ~Transport() {
            for(std::pair<const int, TransportEndpoint *> const & ep : endpoints_) {
                delete ep.second;
            }
        }

        /**
         * Associate each endpoint with a unique id
         */
        void AddEndpoint(const int id, TransportEndpoint *ep) {
            endpoints_[id] = ep;
        }

        int AddPutRequest(const TransportPutMessage *message) {
            return endpoints_.at(message->dst)->AddPutRequest(message);
        }

        int AddGetRequest(const TransportGetMessage *message) {
            return endpoints_.at(message->dst)->AddGetRequest(message);
        }

        int AddPutReply(const int id, TransportRecvMessage **message) {
            return endpoints_.at(id)->AddPutReply(message);
        }

        int AddGetReply(const int id, TransportGetRecvMessage **message) {
            return endpoints_.at(id)->AddGetReply(message);
        }

        int ID() const {
            return id_;
        }

        // virtual TransportEndpointGroup *EndpointGroup() {
        //     return endpointgroup_;
        // }

    private:
        const int id_;

        std::map<int, TransportEndpoint *> endpoints_;
        // TransportEndpointGroup *endpointgroup_;
};

#endif //HXHIM_TRANSPORT
