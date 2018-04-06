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

// /**
//  * An abstract group of communication endpoints
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
        Transport(const int endpoint_id)
            : endpoints_(),
              endpoint_id_(endpoint_id)
        {}

        ~Transport() {
            for(std::pair<const int, TransportEndpoint *> const & ep : endpoints_) {
                delete ep.second;
            }
        }

        /**
         * AddEndpoint
         * Associate each endpoint with a unique id
         *
         * @param id the ID that the given endpoint is associated with
         * @param ep the endpoint containing the transport functionality to send and receive data
         */
        void AddEndpoint(const int id, TransportEndpoint *ep) {
            endpoints_[id] = ep;
        }

        /**
         * RemoveEndpoint
         * Deallocates and removes the endpoint from the transport
         *
         * @param id the ID of the endpoint
         */
        void RemoveEndpoint(const int id) {
            std::map<int, TransportEndpoint *>::iterator it = endpoints_.find(id);
            if (it != endpoints_.end()) {
                delete it->second;
                endpoints_.erase(id);
            }
        }

        /**
         * Put
         * PUTs a message onto the the underlying transport
         *
         * @param put the message to PUT
         * @return the response from the range server
         */
        TransportRecvMessage *Put(const TransportPutMessage *put) {
            std::map<int, TransportEndpoint *>::iterator it = endpoints_.find(put->dst);
            return (it == endpoints_.end())?nullptr:it->second->Put(put);
        }

        /**
         * Get
         * GETs a message onto the the underlying transport
         *
         * @param get the message to GET
         * @return the response from the range server
         */
        TransportGetRecvMessage *Get(const TransportGetMessage *get) {
            std::map<int, TransportEndpoint *>::iterator it = endpoints_.find(get->dst);
            return (it == endpoints_.end())?nullptr:it->second->Get(get);
        }

        /**
         * operator[]
         * Utility function used to get endpoints associated with the given ID
         *
         * @param id an endpoint ID
         * @return pointer to the endpoint associated with the ID, or nullptr
         */
        TransportEndpoint *operator[](const int id) {
            std::map<int, TransportEndpoint *>::iterator it = endpoints_.find(id);
            return (it == endpoints_.end())?nullptr:it->second;
        }

        /**
         * EndpointID
         *
         * @return the ID that this Transport would have if it were an ID
         */
        int EndpointID() const {
            return endpoint_id_;
        }

        // virtual TransportEndpointGroup *EndpointGroup() {
        //     return endpointgroup_;
        // }

    private:
        std::map<int, TransportEndpoint *> endpoints_;
        const int endpoint_id_; // the endpoint id that other processes know this process as
        // TransportEndpointGroup *endpointgroup_;
};

#endif //HXHIM_TRANSPORT
