//
// Created by bws on 8/24/17.
//

#ifndef TRANSPORT_HPP
#define TRANSPORT_HPP

#include <cstdlib>
#include <map>
#include <type_traits>

#include "transport/Messages/Messages.hpp"
#include "transport/constants.h"

namespace Transport {

/**
 * @ An abstract communication endpoint
 */
class Endpoint {
    public:
        virtual ~Endpoint() {}

        /** @description Send a Put to this endpoint */
        virtual Response::Put *Put(const Request::Put *message) = 0;

        /** @description Send a Get to this endpoint */
        virtual Response::Get *Get(const Request::Get *message) = 0;

        /** @description Send a Delete to this endpoint */
        virtual Response::Delete *Delete(const Request::Delete *message) = 0;

        virtual Response::Histogram *Histogram(const Request::Histogram *message) = 0;

    protected:
        Endpoint() {}
        Endpoint(const Endpoint&  rhs) = delete;
        Endpoint(const Endpoint&& rhs) = delete;
};

/**
 * An abstract group of communication endpoints
 */
class EndpointGroup {
    public:
        virtual ~EndpointGroup() {}

        /** @description Bulk Put to multiple endpoints    */
        virtual Response::BPut *BPut(const std::size_t num_rangesrvs, Request::BPut **bpm_list) = 0;

        /** @description Bulk Get from multiple endpoints  */
        virtual Response::BGet *BGet(const std::size_t num_rangesrvs, Request::BGet **bgm_list) = 0;

        /** @description Bulk Get from multiple endpoints  */
        virtual Response::BGetOp *BGetOp(const std::size_t num_rangesrvs, Request::BGetOp **bgm_list) = 0;

        /** @description Bulk Delete to multiple endpoints */
        virtual Response::BDelete *BDelete(const std::size_t num_rangesrvs, Request::BDelete **bdm_list) = 0;

        /** @description Bulk Histogram to multiple endpoints */
        virtual Response::BHistogram *BHistogram(const std::size_t num_rangesrvs, Request::BHistogram **bhist_list) = 0;

   protected:
        EndpointGroup() {}
        EndpointGroup(const EndpointGroup&  rhs) = delete;
        EndpointGroup(const EndpointGroup&& rhs) = delete;

        /**
         * get_num_srsv
         * get the number of servers that will be sent work, and need to be waited on
         *
         * @param messages      the messages that are to be sent
         * @param num_rangesrvs the total number of range servers
         * @param srvs          address of an array that will be created and filled with unique range server IDs
         * @return the          number of unique IDs in *srvs
         */
        template <typename T, typename = std::enable_if_t<std::is_convertible<T, Transport::Message>::value> >
        std::size_t get_num_srvs(T **messages, const std::size_t num_rangesrvs, int **srvs) {
            if (!messages || !srvs) {
                return 0;
            }

            *srvs = nullptr;

            // get the actual number of servers
            int num_srvs = 0;
            *srvs = new int[num_rangesrvs]();
            for (std::size_t i = 0; i < num_rangesrvs; i++) {
                if (!messages[i]) {
                    continue;
                }

                // store server IDs to receive frome
                (*srvs)[num_srvs] = messages[i]->dst;
                num_srvs++;
            }

            if (!num_srvs) {
                delete [] *srvs;
                *srvs = nullptr;
            }

            return num_srvs;
        }
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

        /** @description Takes ownership of an endpoint and associates it with a unique id   */
        void AddEndpoint(const int id, Endpoint *ep);

        /** @description Deallocates and removes the endpoint from the transport             */
        void RemoveEndpoint(const int id);

        /** @description Takes ownership of an endpoint group, deallocating the previous one */
        void SetEndpointGroup(EndpointGroup *eg);

        /**  @description Puts a message onto the the underlying transport    */
        Response::Put *Put(const Request::Put *pm);

        /**  @description Gets a message onto the the underlying transport    */
        Response::Get *Get(const Request::Get *gm);

        /**  @description Deletes a message onto the the underlying transport */
        Response::Delete *Delete(const Request::Delete *dm);

        /** @description Sends a histogram request onto the underlying transport */
        Response::Histogram *Histogram(const Request::Histogram *hist);

        /** @description Bulk Put to multiple endpoints     */
        Response::BPut *BPut(const std::size_t num_rangesrvs, Request::BPut **bpm_list);

        /** @description Bulk Get from multiple endpoints   */
        Response::BGet *BGet(const std::size_t num_rangesrvs, Request::BGet **bgm_list);

        /** @description Bulk Get from multiple endpoints   */
        Response::BGetOp *BGetOp(const std::size_t num_rangesrvs, Request::BGetOp **bgm_list);

        /** @description Bulk Delete to multiple endpoints  */
        Response::BDelete *BDelete(const std::size_t num_rangesrvs, Request::BDelete **bdm_list);

        /** @description Bulk Histogram to multiple endpoints  */
        Response::BHistogram *BHistogram(const std::size_t num_rangesrvs, Request::BHistogram **bdm_list);

    private:
        typedef std::map<int, Endpoint *> EndpointMapping_t;

        EndpointMapping_t endpoints_;
        EndpointGroup *endpointgroup_;
};

}

#endif //HXHIM_TRANSPORT
