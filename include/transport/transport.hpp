//
// Created by bws on 8/24/17.
//

#ifndef TRANSPORT_HPP
#define TRANSPORT_HPP

#include <unordered_map>
#include <map>

#include "transport/Messages/Messages.hpp"
#include "transport/constants.hpp"

namespace Transport {

template <typename Req>
using ReqList = std::unordered_map<int, Req *>;

/**
 * An abstract group of communication endpoints
 */
class EndpointGroup {
    public:
        virtual ~EndpointGroup();

        /** @description Bulk Put to multiple endpoints       */
        virtual Response::BPut *communicate(const ReqList<Request::BPut> &bpm_list);

        /** @description Bulk Get from multiple endpoints     */
        virtual Response::BGet *communicate(const ReqList<Request::BGet> &bgm_list);

        /** @description Bulk Get from multiple endpoints     */
        virtual Response::BGetOp *communicate(const ReqList<Request::BGetOp> &bgm_list);

        /** @description Bulk Delete to multiple endpoints    */
        virtual Response::BDelete *communicate(const ReqList<Request::BDelete> &bdm_list);

        /** @description Bulk Histogram to multiple endpoints */
        virtual Response::BHistogram *communicate(const ReqList<Request::BHistogram> &bhm_list);

   protected:
        EndpointGroup();
        EndpointGroup(const EndpointGroup&  rhs) = delete;
        EndpointGroup(const EndpointGroup&& rhs) = delete;
};

/** Base type for backend specific request handling */
class RangeServer {
    public:
        virtual ~RangeServer();
};

/**
 * Transport interface for endpoints
 * The endpoints and endpoint group do not have to
 * use the same underlying transport protocols.
 *
 * Transport takes ownership of the endpoints,
 * endpointgroup and range server.
 *
 * The range server is only defined if this
 * rank is a range server.
 */
class Transport {
    public:
        Transport(EndpointGroup *epg = nullptr, RangeServer *rs = nullptr);
        ~Transport();

        /** @description Takes ownership of an endpoint group, deallocating the previous one */
        void SetEndpointGroup(EndpointGroup *eg);

        /** @description Takes ownership of a range server, deallocating the previous one */
        void SetRangeServer(RangeServer *rs);

        /** @description Bulk Put to multiple endpoints        */
        Response::BPut *communicate(const ReqList<Request::BPut> &bpm_list);

        /** @description Bulk Get from multiple endpoints      */
        Response::BGet *communicate(const ReqList<Request::BGet> &bgm_list);

        /** @description Bulk Get from multiple endpoints      */
        Response::BGetOp *communicate(const ReqList<Request::BGetOp> &bgm_list);

        /** @description Bulk Delete to multiple endpoints     */
        Response::BDelete *communicate(const ReqList<Request::BDelete> &bdm_list);

        /** @description Bulk Histogram to multiple endpoints  */
        Response::BHistogram *communicate(const ReqList<Request::BHistogram> &bhm_list);

    private:
        EndpointGroup *endpointgroup_;
        RangeServer *rangeserver_;
};

}

#endif //HXHIM_TRANSPORT
