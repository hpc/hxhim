//
// Created by bws on 8/24/17.
//

#ifndef HXHIM_TRANSPORT
#define HXHIM_TRANSPORT

#include <cstdlib>
#include <functional>
#include <string>

#include "mdhim_constants.h"
#include "mdhim_struct.h"
#include "transport_constants.h"

/**
 * @description An abstract communication address
 */
class TransportAddress {
    public:
        TransportAddress() {}
        TransportAddress(const TransportAddress& rhs) {}
        virtual ~TransportAddress() {};

        bool operator==(const TransportAddress &rhs) {
            return equals(rhs);
        }

        /*
           Function for typecasting TransportAddresses
           into octet buffers
         */
        virtual operator std::string() const = 0;

        /*
          Function for typecasting TransportAddresses
          into ints

          TODO: Remove this function once MPI is properly abstracted
         */
        virtual operator int() const = 0;

    protected:
        virtual bool equals(const TransportAddress &rhs) const = 0;
};

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
        int server_rank; // this should eventually become TransportAddress
        int index;
        int index_type;
        char *index_name;
};

/**
 * TransportPutMessage
 * Put message implementation
 */
class TransportPutMessage final : public TransportMessage {
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
class TransportBPutMessage final : public TransportMessage {
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
class TransportGet : public TransportMessage {
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
class TransportGetMessage final : public TransportGet {
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
class TransportBGetMessage final : public TransportGet {
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
class TransportDeleteMessage final : public TransportMessage {
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
class TransportBDeleteMessage final : public TransportMessage {
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
class TransportRecvMessage final : public TransportMessage {
    public:
        TransportRecvMessage();
        ~TransportRecvMessage();

        int size() const;
        void cleanup();

        int error;
};

/**
 * TransportBGetRecvMessage
 * Bulk get receive message implementation
 */
class TransportBGetRecvMessage final : public TransportMessage {
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
class TransportBRecvMessage final : public TransportMessage {
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

        /** @description Ensure queued messages are sent to destination and serviced */
        virtual int Flush() = 0;

        /** @description Enqueue a Put request for this endpoint */
        virtual int AddPutRequest(const TransportPutMessage *message) = 0;

        /** @description Enqueue a Get request for this endpoint */
        virtual int AddGetRequest(const TransportBGetMessage *message) = 0;

        /** @description Enqueue a Put reply for the request originator */
        virtual int AddPutReply(const TransportAddress *src, TransportRecvMessage **message) = 0;

        /** @description Enqueue a Get reply for the request originator */
        virtual int AddGetReply(const TransportAddress *src, TransportBGetRecvMessage ***messages) = 0;

        /** @description Poll for number of seconds to determine if a message is present */
        virtual std::size_t PollForMessage(std::size_t timeoutSecs) = 0;

        /** @description Wait for number of seconds for an interrupt to indicate a message is present*/
        virtual std::size_t WaitForMessage(std::size_t timeoutSecs) = 0;

        virtual const TransportAddress *Address() const = 0;

    protected:
        TransportEndpoint() {}

        TransportEndpoint(const TransportEndpoint& rhs) {}
};

/**
 * An abstract group of communication endpoints
 * @param Transport
 */
class TransportEndpointGroup {
    public:
        virtual ~TransportEndpointGroup() {}

        /** @description Enqueue a BPut requests to multiple endpoints  */
        virtual int AddBPutRequest(TransportBPutMessage **messages, int num_srvs) = 0;

        /** @description Enqueue a BGet request to multiple endpoints  */
        virtual int AddBGetRequest(TransportBGetMessage **messages, int num_srvs) = 0;

        /** @description Enqueue a BPut reply for the request originator */
        virtual int AddBPutReply(const TransportAddress *srcs, int nsrcs, TransportRecvMessage **message) = 0;

        /** @description Enqueue a BGet reply for the request originator */
        virtual int AddBGetReply(const TransportAddress *srcs, int nsrcs, TransportBGetRecvMessage **messages) = 0;

        virtual const TransportAddress *Address() const = 0;

   protected:
        TransportEndpointGroup() {}

        TransportEndpointGroup(const TransportEndpointGroup& rhs){}
};

/**
 * Abstract base for all Transport Implementations (MPI, Sockets, Mercury, etc.)
 * Transport takes ownership of the EP and EG pointers
 */
class Transport {
    public:
        Transport(TransportEndpoint *ep, TransportEndpointGroup *eg)
          : endpoint_(ep), endpointgroup_(eg)
        {}

        virtual ~Transport() {
            delete endpoint_;
            delete endpointgroup_;
        }

        virtual TransportEndpoint *Endpoint() {
            return endpoint_;
        }

        virtual TransportEndpointGroup *EndpointGroup() {
            return endpointgroup_;
        }

    private:
        TransportEndpoint *endpoint_;
        TransportEndpointGroup *endpointgroup_;
};

/**
 * function signature that will be used for
 * acknowledging mdhim operations
 */
typedef std::function<int(Transport *, const TransportAddress *, TransportMessage *)> TransportResponseSender;

/**
 * function signature that will be used for
 * receiving initial mdhim operation
 */
typedef std::function<int(Transport *, TransportAddress**, TransportMessage **)> TransportWorkReceiver;

#endif //HXHIM_TRANSPORT
