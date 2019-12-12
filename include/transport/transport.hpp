//
// Created by bws on 8/24/17.
//

#ifndef TRANSPORT_HPP
#define TRANSPORT_HPP

#include <unordered_map>

#include "transport/messages/Messages.hpp"
#include "transport/constants.hpp"

namespace Transport {

/**
 * An abstract group of communication endpoints
 */
class EndpointGroup {
    public:
        virtual ~EndpointGroup();

        /** @description Bulk Put to multiple endpoints    */
        virtual Response::BPut *communicate(const std::unordered_map<int, Request::BPut *> &bpm_list);

        /** @description Bulk Get from multiple endpoints  */
        virtual Response::BGet *communicate(const std::unordered_map<int, Request::BGet *> &bgm_list);

        /** @description Bulk Get from multiple endpoints  */
        virtual Response::BGetOp *communicate(const std::unordered_map<int, Request::BGetOp *> &bgm_list);

        /** @description Bulk Delete to multiple endpoints */
        virtual Response::BDelete *communicate(const std::unordered_map<int, Request::BDelete *> &bdm_list);

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
        Response::BPut *communicate(const std::unordered_map<int, Request::BPut *> &bpm_list);

        /** @description Bulk Get from multiple endpoints      */
        Response::BGet *communicate(const std::unordered_map<int, Request::BGet *> &bgm_list);

        /** @description Bulk Get from multiple endpoints      */
        Response::BGetOp *communicate(const std::unordered_map<int, Request::BGetOp *> &bgm_list);

        /** @description Bulk Delete to multiple endpoints     */
        Response::BDelete *communicate(const std::unordered_map<int, Request::BDelete *> &bdm_list);

        // /** @description Bulk Histogram to multiple endpoints  */
        // Response::BHistogram *communicate(const std::unordered_map<int, Request::BHistogram *> &bhm_list);

    private:
        EndpointGroup *endpointgroup_;
};

}

#endif //HXHIM_TRANSPORT
