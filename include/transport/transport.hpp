//
// Created by bws on 8/24/17.
//

#ifndef TRANSPORT_HPP
#define TRANSPORT_HPP

#include <atomic>
#include <list>
#include <map>
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
        virtual Response::BPut *BPut(const std::map<int, Request::BPut *> &bpm_list);

        /** @description Bulk Get from multiple endpoints  */
        virtual Response::BGet *BGet(const std::map<int, Request::BGet *> &bgm_list);

        /** @description Bulk Get from multiple endpoints  */
        virtual Response::BGetOp *BGetOp(const std::map<int, Request::BGetOp *> &bgm_list);

        /** @description Bulk Delete to multiple endpoints */
        virtual Response::BDelete *BDelete(const std::map<int, Request::BDelete *> &bdm_list);

        /** @description Bulk Histogram to multiple endpoints */
        virtual Response::BHistogram *BHistogram(const std::map<int, Request::BHistogram *> &bhm_list);

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

        /** @description Deletes a message onto the the underlying transport     */
        Response::Delete *Delete(const Request::Delete *dm);

        /** @description Sends a histogram request onto the underlying transport */
        Response::Histogram *Histogram(const Request::Histogram *hm);

        /** @description Bulk Put to multiple endpoints        */
        Response::BPut *BPut(const std::map<int, Request::BPut *> &bpm_list);

        /** @description Bulk Get from multiple endpoints      */
        Response::BGet *BGet(const std::map<int, Request::BGet *> &bgm_list);

        /** @description Bulk Get from multiple endpoints      */
        Response::BGetOp *BGetOp(const std::map<int, Request::BGetOp *> &bgm_list);

        /** @description Bulk Delete to multiple endpoints     */
        Response::BDelete *BDelete(const std::map<int, Request::BDelete *> &bdm_list);

        /** @description Bulk Histogram to multiple endpoints  */
        Response::BHistogram *BHistogram(const std::map<int, Request::BHistogram *> &bhm_list);

        /** @decription Statistics for an instance of Transport */
        struct  Stats {
            struct Op {
                /** @description Statistics on percentage of each packet filled */
                struct Filled {
                    Filled(const int dst, const long double percent)
                        : dst(dst),
                          percent(percent)
                    {}

                    Filled(const Filled &filled)
                        : dst(filled.dst),
                          percent(filled.percent)
                    {}

                    Filled(const Filled &&filled)
                        : dst(std::move(filled.dst)),
                          percent(std::move(filled.percent))
                    {}

                    Filled &operator=(const Filled &filled) {
                        dst = filled.dst;
                        percent = filled.percent;
                        return *this;
                    }

                    Filled &operator==(const Filled &&filled) {
                        dst = std::move(filled.dst);
                        percent = std::move(filled.percent);
                        return *this;
                    }

                    int dst;
                    long double percent;
                };

                std::list<Filled> filled;
                mutable std::mutex mutex;
            };

            Op bput;
            Op bget;
            Op bgetop;
            Op bdel;
        };

        int GetAverageFilled(MPI_Comm comm, const int rank, const int dst_rank,
                             const bool get_bput, long double *bput,
                             const bool get_bget, long double *bget,
                             const bool get_bgetop, long double *bgetop,
                             const bool get_bdel, long double *bdel) const;

        int GetMinFilled(MPI_Comm comm, const int rank, const int dst_rank,
                         const bool get_bput, long double *bput,
                         const bool get_bget, long double *bget,
                         const bool get_bgetop, long double *bgetop,
                         const bool get_bdel, long double *bdel) const;

        int GetMaxFilled(MPI_Comm comm, const int rank, const int dst_rank,
                         const bool get_bput, long double *bput,
                         const bool get_bget, long double *bget,
                         const bool get_bgetop, long double *bgetop,
                         const bool get_bdel, long double *bdel) const;

    private:
        typedef std::map<int, Endpoint *> EndpointMapping_t;

        EndpointMapping_t endpoints_;
        EndpointGroup *endpointgroup_;

        Stats stats;
};

}

#endif //HXHIM_TRANSPORT
