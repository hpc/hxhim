//
// Created by bws on 8/24/17.
//

#ifndef HXHIM_COMM_H

#define HXHIM_COMM_H

#include <cstdlib>
#include <string>

/**
 * @description Message types
 */
class CommMessage {
public:
    enum Type {
        INVALID = 0,
        GET,
        PUT,
        BGET,
        BPUT
    };
};


/**
 * @description An abstract communication address
 */
class CommAddress {
public:
    /** @brief Destructor */
    virtual ~CommAddress() {};

    /** */
    CommAddress() {}

    /** */
    CommAddress(const CommAddress& other) {}

    /*
       Function for typecasting CommAddresses
       into octet buffers
     */
    virtual operator std::string() const = 0;

    /*
      Function for typecasting CommAddresses
      into ints

      TODO: Remove this function once MPI is properly abstracted
     */
    virtual operator int() const = 0;
};

/**
 * @ An abstract communication endpoint
 *
 * This is a stateful API that allows the user to enqueue PUT and GET operations,
 * and use the flush call to complete progress. You initiate a Put or Get and receive an operation id. You then
 * keep calling the function until the operation is completed.
 */
class CommEndpoint {
    public:
        /** @brief Destructor */
        virtual ~CommEndpoint() {}

        /** @description Ensure queued messages are sent to destination and serviced */
        virtual int Flush() = 0;

        /** @description Enqueue a Put request for this endpoint */
        virtual int AddPutRequest(struct mdhim_putm_t *pm) = 0;

        /** @description Enqueue a Get request for this endpoint */
        virtual int AddGetRequest(void* kbuf, std::size_t kbytes, void* vbuf, std::size_t vbytes) = 0;

        /** @description Enqueue a Put reply for the request originator */
        virtual int AddPutReply(const CommAddress *src, void **message) = 0;

        /** @description Enqueue a Get reply for the request originator */
        virtual int AddGetReply(void* vbuf, std::size_t vbytes) = 0;

        /** @description Immediately receive a PUT or GET request sent to this endpoint */
        virtual int ReceiveRequest(std::size_t rbytes, CommMessage::Type* requestType, void** kbuf, std::size_t* kbytes, void** vbuf, std::size_t* vbytes) = 0;

        /** @description Poll for number of seconds to determine if a message is present */
        virtual std::size_t PollForMessage(std::size_t timeoutSecs) = 0;

        /** @description Wait for number of seconds for an interrupt to indicate a message is present*/
        virtual std::size_t WaitForMessage(std::size_t timeoutSecs) = 0;

        virtual CommEndpoint *Dup() const = 0;

        virtual const CommAddress *Address() const = 0;

    protected:
        /** Constructor */
        CommEndpoint() {}

        /** @brief Copy Constructor */
        CommEndpoint(const CommEndpoint& other) {};
};

/**
 * An abstract group of communication endpoints
 * @tparam Transport
 */
class CommEndpointGroup {
    public:
        virtual ~CommEndpointGroup() {}

        /** @description Send messages to multiple endpoints */
        virtual int BulkPutMessage() = 0;

        /** @description Recv messages from multiple endpoint */
        virtual int BulkGetMessage() = 0;

        virtual CommEndpointGroup *Dup() const = 0;

        virtual const CommAddress *Address() const = 0;

   protected:
        /** Constructor */
        CommEndpointGroup() {};

        /** Copy Constructor */
        CommEndpointGroup(const CommEndpointGroup& other) {};
};

/**
 * Abstract base for all Comm Implementations (MPI, Sockets, Mercury, etc.)
 * CommTransport takes ownership of the EP and EG pointers
 */
class CommTransport {
    public:
        CommTransport(CommEndpoint *ep, CommEndpointGroup *eg)
          : endpoint_(ep), endpointgroup_(eg)
        {}

        ~CommTransport() {
            delete endpoint_;
            delete endpointgroup_;
        }

        CommEndpoint *Endpoint() {
            return endpoint_;
        }

        CommEndpointGroup *EndpointGroup() {
            return endpointgroup_;
        }

        CommTransport *Dup() const {
            return new CommTransport(endpoint_->Dup(), endpointgroup_->Dup());
        }

    private:
        CommEndpoint *endpoint_;
        CommEndpointGroup *endpointgroup_;
};

#endif //HXHIM_COMM_H
