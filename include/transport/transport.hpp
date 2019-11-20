//
// Created by bws on 8/24/17.
//

#ifndef TRANSPORT_HPP
#define TRANSPORT_HPP

#include <atomic>
#include <list>
#include <unordered_map>
#include <mutex>
#include <vector>

#include <mpi.h>

#include "transport/Messages/Messages.hpp"
#include "transport/constants.hpp"

namespace Transport {

/**
 * @ An abstract communication endpoint
 */
class Endpoint {
    public:
       virtual ~Endpoint();

        /** @description Send a Put to this endpoint */
        virtual Response::Put *Put(const Request::Put *message);

        /** @description Send a Get to this endpoint */
        virtual Response::Get *Get(const Request::Get *message);

        /** @description Send a Get to this endpoint */
        virtual Response::Get2 *Get2(const Request::Get2 *message);

        /** @description Send a Delete to this endpoint */
        virtual Response::Delete *Delete(const Request::Delete *message);

        virtual Response::Histogram *Histogram(const Request::Histogram *message);

    protected:
        Endpoint();
        Endpoint(const Endpoint&  rhs) = delete;
        Endpoint(const Endpoint&& rhs) = delete;
};

/**
 * An abstract group of communication endpoints
 */
class EndpointGroup {
    public:
        virtual ~EndpointGroup();

        /** @description Bulk Put to multiple endpoints    */
        virtual Response::BPut *BPut(const std::unordered_map<int, Request::BPut *> &bpm_list);

        /** @description Bulk Get from multiple endpoints  */
        virtual Response::BGet *BGet(const std::unordered_map<int, Request::BGet *> &bgm_list);

        /** @description Bulk Get from multiple endpoints  */
        virtual Response::BGet2 *BGet2(const std::unordered_map<int, Request::BGet2 *> &bgm_list);

        /** @description Bulk Get from multiple endpoints  */
        virtual Response::BGetOp *BGetOp(const std::unordered_map<int, Request::BGetOp *> &bgm_list);

        /** @description Bulk Delete to multiple endpoints */
        virtual Response::BDelete *BDelete(const std::unordered_map<int, Request::BDelete *> &bdm_list);

        /** @description Bulk Histogram to multiple endpoints */
        virtual Response::BHistogram *BHistogram(const std::unordered_map<int, Request::BHistogram *> &bhm_list);

   protected:
        EndpointGroup();
        EndpointGroup(const EndpointGroup&  rhs) = delete;
        EndpointGroup(const EndpointGroup&& rhs) = delete;
};

/**
 * Transport interface for endpoints
 * The endpoints and endpoint group do not have to
 * use the same underlying transport protocols.
 *
 * Transport takes ownership of the endpoints,
 * endpointgroup and range server.
 */
class Transport {
    public:
        Transport();
        ~Transport();

        /** @description Takes ownership of an endpoint and associates it with a unique id   */
        void AddEndpoint(const int id, Endpoint *ep);

        /** @description Deallocates and removes the endpoint from the transport             */
        void RemoveEndpoint(const int id);

        /** @description Takes ownership of an endpoint group, deallocating the previous one */
        void SetEndpointGroup(EndpointGroup *eg);

        /** @description Puts a message onto the the underlying transport        */
        Response::Put *Put(const Request::Put *pm);

        /** @description Gets a message onto the the underlying transport        */
        Response::Get *Get(const Request::Get *gm);

        /** @description Get2s a message onto the the underlying transport        */
        Response::Get2 *Get2(const Request::Get2 *gm);

        /** @description Deletes a message onto the the underlying transport     */
        Response::Delete *Delete(const Request::Delete *dm);

        /** @description Sends a histogram request onto the underlying transport */
        Response::Histogram *Histogram(const Request::Histogram *hm);

        /** @description Bulk Put to multiple endpoints        */
        Response::BPut *BPut(const std::unordered_map<int, Request::BPut *> &bpm_list);

        /** @description Bulk Get from multiple endpoints      */
        Response::BGet *BGet(const std::unordered_map<int, Request::BGet *> &bgm_list);

        /** @description Bulk Get2 from multiple endpoints      */
        Response::BGet2 *BGet2(const std::unordered_map<int, Request::BGet2 *> &bgm_list);

        /** @description Bulk Get from multiple endpoints      */
        Response::BGetOp *BGetOp(const std::unordered_map<int, Request::BGetOp *> &bgm_list);

        /** @description Bulk Delete to multiple endpoints     */
        Response::BDelete *BDelete(const std::unordered_map<int, Request::BDelete *> &bdm_list);

        /** @description Bulk Histogram to multiple endpoints  */
        Response::BHistogram *BHistogram(const std::unordered_map<int, Request::BHistogram *> &bhm_list);

    private:
        typedef std::unordered_map<int, Endpoint *> EndpointUnordered_Mapping_t;

        EndpointUnordered_Mapping_t endpoints_;
        EndpointGroup *endpointgroup_;
};

}

#endif //HXHIM_TRANSPORT
