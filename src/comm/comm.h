//
// Created by bws on 8/24/17.
//

#ifndef HXHIM_COMM_H

#define HXHIM_COMM_H

#include <cstdlib>

/**
 * @description An abstract communication address
 */
class CommAddress {
public:
    /** @brief Destructor */
    virtual ~CommAddress() = 0;

protected:
    /** */
    CommAddress() {}

    /** */
    CommAddress(const CommAddress& other) {}

    /** @return an array of bytes that can be cast into the native transport address format */
    std::size_t Native(void** buf) const;

    /**
     * @brief Set the native address representation
     * @return 0 for success, non-zero on error
     * */
    virtual int Native(void* buf, std::size_t nbytes) const = 0;

private:


};

inline CommAddress::~CommAddress() {}

/**
 * @ An abstract communication endpoint
 */
class CommEndpoint {
public:
    /** @brief Destructor */
    virtual ~CommEndpoint() {}

    /** @description Send a message to another endpoint */
    int PutMessage(const CommEndpoint& dest) { return this->PutMessageImpl(dest); }

    /** @description Send a message to this endpoint */
    int GetMessage(const CommEndpoint& src) { return this->GetMessageImpl(src); }

    int PollForMessage(std::size_t timeoutSecs) { return this->PollForMessageImpl(timeoutSecs); }

    int WaitForMessage(std::size_t timeoutSecs) { return this->WaitForMessageImpl(timeoutSecs); }

protected:
    /** Constructor */
    CommEndpoint() {}

    /** @brief Copy Constructor */
    CommEndpoint(const CommEndpoint& other) {};

    virtual int PutMessageImpl(const CommEndpoint& dest) = 0;

    virtual int GetMessageImpl(const CommEndpoint& src) = 0;

    virtual int PollForMessageImpl(std::size_t timeoutSecs) = 0;

    virtual int WaitForMessageImpl(std::size_t timeoutSecs) = 0;

private:
};

/**
 * An abstract group of communication endpoints
 * @tparam Transport
 */
class CommEndpointGroup {
public:
    /** @description Send messages to multiple endpoints */
    int BulkPutMessage() { return this->ScatterImpl(); }

    /** @description Recv messages from multiple endpoint */
    int BulkGetMessage() { return this->GatherImpl(); }

protected:
    /** Constructor */
    CommEndpointGroup() {};

    /** Copy Constructor */
    CommEndpointGroup(const CommEndpointGroup& other) {};

    virtual int GatherImpl() = 0;

    virtual int ScatterImpl() = 0;
};

/**
 * Abstract base for all Comm Implementations (MPI, Sockets, Mercury, etc.)
 */
class CommTransport {
public:
    //int CreateAddress(A* addr) const { return this->createAddressImpl(A* addr); }
    //int CreateEndpoint(EP* ep) const { return this->createEndpointImpl(EP* endpoint); }
    //int CreateEndpointGroup(EG* epGroup) const {};
protected:
    CommTransport() {}
    ~CommTransport() {}
};

/**
 * Specialization class for creating a CommTransport implementation
 * @tparam A A concrete CommAddress type
 * @tparam EP A concrete Endpoint type
 * @tparam EG A concrete EndpointGroup type
 */
template <class A, class EP, class EG>
class CommTransportSpecialization : public CommTransport {
public:

};

#endif //HXHIM_COMM_H
