//
// Created by bws on 8/24/17.
//

#ifndef TRANSPORT_HPP
#define TRANSPORT_HPP

#include <unordered_map>

#include "transport/Messages/Messages.hpp"
#include "transport/constants.hpp"

namespace Transport {

/**
 * An abstract group of communication endpoints
 */
class EndpointGroup {
    public:
        virtual ~EndpointGroup();

        /** @description Bulk Put to multiple endpoints    */
        virtual Response::RecvBPut *communicate(const std::unordered_map<int, Request::SendBPut *> &bpm_list);

        /** @description Bulk Get from multiple endpoints  */
        virtual Response::RecvBGet *communicate(const std::unordered_map<int, Request::SendBGet *> &bgm_list);

        /** @description Bulk Get from multiple endpoints  */
        virtual Response::RecvBGetOp *communicate(const std::unordered_map<int, Request::SendBGetOp *> &bgm_list);

        /** @description Bulk Delete to multiple endpoints */
        virtual Response::RecvBDelete *communicate(const std::unordered_map<int, Request::SendBDelete *> &bdm_list);

        // /** @description Bulk Histogram to multiple endpoints */
        // virtual Response::BHistogram *communicate(const std::unordered_map<int, Request::BHistogram *> &bhm_list);

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

        /** @description Takes ownership of an endpoint group, deallocating the previous one */
        void SetEndpointGroup(EndpointGroup *eg);

        /** @description Bulk Put to multiple endpoints        */
        Response::RecvBPut *communicate(const std::unordered_map<int, Request::SendBPut *> &bpm_list);

        /** @description Bulk Get from multiple endpoints      */
        Response::RecvBGet *communicate(const std::unordered_map<int, Request::SendBGet *> &bgm_list);

        /** @description Bulk Get from multiple endpoints      */
        Response::RecvBGetOp *communicate(const std::unordered_map<int, Request::SendBGetOp *> &bgm_list);

        /** @description Bulk Delete to multiple endpoints     */
        Response::RecvBDelete *communicate(const std::unordered_map<int, Request::SendBDelete *> &bdm_list);

        // /** @description Bulk Histogram to multiple endpoints  */
        // Response::BHistogram *communicate(const std::unordered_map<int, Request::BHistogram *> &bhm_list);

    private:
        EndpointGroup *endpointgroup_;
};

}

#endif //TRANSPORT
