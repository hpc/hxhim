#include <cmath>
#include <cfloat>

#include "transport/transport.hpp"
#include "utils/macros.hpp"

namespace Transport {

Endpoint::Endpoint() {}

Endpoint::~Endpoint() {}

Response::Put *Endpoint::Put(const Request::Put *) {
    return nullptr;
}

Response::Get *Endpoint::Get(const Request::Get *) {
    return nullptr;
}

Response::Delete *Endpoint::Delete(const Request::Delete *) {
    return nullptr;
}

Response::Histogram *Endpoint::Histogram(const Request::Histogram *) {
    return nullptr;
}

EndpointGroup::EndpointGroup() {}

EndpointGroup::~EndpointGroup() {}

Response::BPut *EndpointGroup::BPut(const std::map<int, Request::BPut *> &) {
    return nullptr;
}

Response::BGet *EndpointGroup::BGet(const std::map<int, Request::BGet *> &) {
    return nullptr;
}

Response::BGetOp *EndpointGroup::BGetOp(const std::map<int, Request::BGetOp *> &) {
    return nullptr;
}

Response::BDelete *EndpointGroup::BDelete(const std::map<int, Request::BDelete *> &) {
    return nullptr;
}

Response::BHistogram *EndpointGroup::BHistogram(const std::map<int, Request::BHistogram *> &) {
    return nullptr;
}

Transport::Transport()
    : endpoints_(),
      endpointgroup_(nullptr)
{}

Transport::~Transport() {
    SetEndpointGroup(nullptr);

    for(decltype(endpoints_)::value_type const & ep : endpoints_) {
        delete ep.second;
    }
}

/**
 * AddEndpoint
 * Takes ownership of an endpoint and associates it with a unique id
 *
 * @param id the ID that the given endpoint is associated with
 * @param ep the endpoint containing the transport functionality to send and receive data
 */
void Transport::AddEndpoint(const int id, Endpoint *ep) {
    endpoints_[id] = ep;
}

/**
 * RemoveEndpoint
 * Deallocates and removes the endpoint from the transport
 *
 * @param id the ID of the endpoint
 */
void Transport::RemoveEndpoint(const int id) {
    EndpointMapping_t::iterator it = endpoints_.find(id);
    if (it != endpoints_.end()) {
        delete it->second;
        endpoints_.erase(id);
    }
}

/**
 * SetEndpointGroup
 * Takes ownership of an endpoint group, deallocating the previous one
 *
 * @param eg the endpoint group to take ownership of
 */
void Transport::SetEndpointGroup(EndpointGroup *eg) {
    if (endpointgroup_) {
        delete endpointgroup_;
    }
    endpointgroup_ = eg;
}

/**
 * Put
 * Puts a message onto the the underlying transport
 *
 * @param pm the message to PUT
 * @return the response from the range server
 */
Response::Put *Transport::Put(const Request::Put *pm) {
    if (!pm) {
        return nullptr;
    }
    EndpointMapping_t::iterator it = endpoints_.find(pm->dst);
    return (it == endpoints_.end())?nullptr:it->second->Put(pm);
}

/**
 * Get
 * Gets a message onto the the underlying transport
 *
 * @param gm the message to GET
 * @return the response from the range server
 */
Response::Get *Transport::Get(const Request::Get *get) {
    if (!get) {
        return nullptr;
    }
    EndpointMapping_t::iterator it = endpoints_.find(get->dst);
    return (it == endpoints_.end())?nullptr:it->second->Get(get);
}

/**
 * Delete
 * Deletes a message onto the the underlying transport
 *
 * @param dm the message to DELETE
 * @return the response from the range server
 */
Response::Delete *Transport::Delete(const Request::Delete *dm) {
    if (!dm) {
        return nullptr;
    }
    EndpointMapping_t::iterator it = endpoints_.find(dm->dst);
    return (it == endpoints_.end())?nullptr:it->second->Delete(dm);
}

/**
 * Histogram
 * Sends a histogram message onto the the underlying transport
 *
 * @param hm the HISTOGRAM message
 * @return the response from the range server
 */
Response::Histogram *Transport::Histogram(const Request::Histogram *hm) {
    if (!hm) {
        return nullptr;
    }
    EndpointMapping_t::iterator it = endpoints_.find(hm->dst);
    return (it == endpoints_.end())?nullptr:it->second->Histogram(hm);
}

/**
 * collect_fill_stats
 * Collects statistics on how much of a bulk packet was used before being sent into the transport
 *
 * @param list the list (really map) of messages
 * @param op   the statistics structure for an operation (PUT, GET, GETOP, DEL)
 */
template <typename T, typename = std::enable_if<std::is_base_of<Bulk,             T>::value &&
                                                std::is_base_of<Request::Request, T>::value> >
void collect_fill_stats(const std::map<int, T *> &list, Transport::Stats::Op &op) {
    // Collect packet filled percentage
    std::lock_guard<std::mutex> lock(op.mutex);
    for(REF(list)::value_type const & msg : list) {
        op.filled.emplace_back(Transport::Transport::Stats::Op::Filled(msg.first, (double) msg.second->count / (double) msg.second->max_count));
    }
}

/**
 * BPut
 * Bulk Put to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bpm_list a list of PUT messages going to different servers
 * @return the response from the range server
 */
Response::BPut *Transport::BPut(const std::map<int, Request::BPut *> &bpm_list) {
    collect_fill_stats(bpm_list, stats.bput);
    return (bpm_list.size() && endpointgroup_)?endpointgroup_->BPut(bpm_list):nullptr;
}

/**
 * BGet
 * Bulk Get to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list a list of GET messages going to different servers
 * @return the response from the range server
 */
Response::BGet *Transport::BGet(const std::map<int, Request::BGet *> &bgm_list) {
    collect_fill_stats(bgm_list, stats.bget);
    return (bgm_list.size() && endpointgroup_)?endpointgroup_->BGet(bgm_list):nullptr;
}

/**
 * BGetOp
 * Bulk Get to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list a list of GET messages going to different servers
 * @return the response from the range server
 */
Response::BGetOp *Transport::BGetOp(const std::map<int, Request::BGetOp *> &bgm_list) {
    collect_fill_stats(bgm_list, stats.bgetop);
    return (bgm_list.size() && endpointgroup_)?endpointgroup_->BGetOp(bgm_list):nullptr;
}

/**
 * BDelete
 * Bulk Delete to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list a list of DELETE messages going to different servers
 * @return the response from the range server
 */
Response::BDelete *Transport::BDelete(const std::map<int, Request::BDelete *> &bdm_list) {
    collect_fill_stats(bdm_list, stats.bget);
    return (bdm_list.size() && endpointgroup_)?endpointgroup_->BDelete(bdm_list):nullptr;
}

/**
 * BHistogram
 * Bulk Histogram to multiple endpoints
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list a list of HISTOGRAM messages going to different servers
 * @return the response from the range server
 */
Response::BHistogram *Transport::BHistogram(const std::map<int, Request::BHistogram *> &bhm_list) {
    return (bhm_list.size() && endpointgroup_)?endpointgroup_->BHistogram(bhm_list):nullptr;
}

/**
 * GetFilled
 * Collective operation
 * Collects statistics from all ranks in the communicator
 *
 * @param comm        the MPI communicator
 * @param rank        the rank of this instance of Transport
 * @param dst_rank    the rank to send to
 * @param get_bput    whether or not to get bput
 * @param bput        the array of bput from each rank
 * @param get_bget    whether or not to get bget
 * @param bget        the array of bget from each rank
 * @param get_bgetop  whether or not to get bgetop
 * @param bgetop      the array of bgetop from each rank
 * @param get_bdel    whether or not to get bdel
 * @param bdel        the array of bdel from each rank
 * @param calc        the function to use to calculate some statistic using the input data
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
 */
static int GetFilled(MPI_Comm comm, const int rank, const int dst_rank,
                     const bool get_bput, long double *bput,
                     const bool get_bget, long double *bget,
                     const bool get_bgetop, long double *bgetop,
                     const bool get_bdel, long double *bdel,
                     const Transport::Transport::Stats &stats,
                     const std::function<long double(const Transport::Transport::Stats::Op &)> &calc) {
    MPI_Barrier(comm);

    if (rank == dst_rank) {
        if (get_bput) {
            const long double filled = calc(stats.bput);
            MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, bput, 1, MPI_LONG_DOUBLE, dst_rank, comm);
        }
        if (get_bget) {
            const long double filled = calc(stats.bget);
            MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, bget, 1, MPI_LONG_DOUBLE, dst_rank, comm);
        }
        if (get_bgetop) {
            const long double filled = calc(stats.bgetop);
            MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, bgetop, 1, MPI_LONG_DOUBLE, dst_rank, comm);
        }
        if (get_bdel) {
            const long double filled = calc(stats.bdel);
            MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, bdel, 1, MPI_LONG_DOUBLE, dst_rank, comm);
        }
    }
    else {
        if (get_bput) {
            const long double filled = calc(stats.bput);
            MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, nullptr, 0, MPI_LONG_DOUBLE, dst_rank, comm);
        }
        if (get_bget) {
            const long double filled = calc(stats.bget);
            MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, nullptr, 0, MPI_LONG_DOUBLE, dst_rank, comm);
        }
        if (get_bgetop) {
            const long double filled = calc(stats.bgetop);
            MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, nullptr, 0, MPI_LONG_DOUBLE, dst_rank, comm);
        }
        if (get_bdel) {
            const long double filled = calc(stats.bdel);
            MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, nullptr, 0, MPI_LONG_DOUBLE, dst_rank, comm);
        }
    }

    MPI_Barrier(comm);

    return TRANSPORT_SUCCESS;
}

/**
 * GetMinFilled
 * Collective operation
 * Collects statistics from all ranks in the communicator
 *
 * @param comm        the MPI communicator
 * @param rank        the rank of this instance of Transport
 * @param dst_rank    the rank to send to
 * @param get_bput    whether or not to get bput
 * @param bput        the array of bput from each rank
 * @param get_bget    whether or not to get bget
 * @param bget        the array of bget from each rank
 * @param get_bgetop  whether or not to get bgetop
 * @param bgetop      the array of bgetop from each rank
 * @param get_bdel    whether or not to get bdel
 * @param bdel        the array of bdel from each rank
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
 */
int Transport::GetMinFilled(MPI_Comm comm, const int rank, const int dst_rank,
                            const bool get_bput, long double *bput,
                            const bool get_bget, long double *bget,
                            const bool get_bgetop, long double *bgetop,
                            const bool get_bdel, long double *bdel) const {
    auto min_filled = [](const Transport::Transport::Stats::Op &op) -> long double {
        std::lock_guard<std::mutex> lock(op.mutex);
        long double min = LDBL_MAX;
        for(REF(op.filled)::value_type const &filled : op.filled) {
            min = std::min(min, filled.percent);
        }

        return min;
    };

    return GetFilled(comm, rank, dst_rank,
                     get_bput, bput,
                     get_bget, bget,
                     get_bgetop, bgetop,
                     get_bdel, bdel,
                     stats,
                     min_filled);
}

/**
 * GetAverageFilled
 * Collective operation
 * Collects statistics from all ranks in the communicator
 *
 * @param comm        the MPI communicator
 * @param rank        the rank of this instance of Transport
 * @param dst_rank    the rank to send to
 * @param get_bput    whether or not to get bput
 * @param bput        the array of bput from each rank
 * @param get_bget    whether or not to get bget
 * @param bget        the array of bget from each rank
 * @param get_bgetop  whether or not to get bgetop
 * @param bgetop      the array of bgetop from each rank
 * @param get_bdel    whether or not to get bdel
 * @param bdel        the array of bdel from each rank
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
 */
int Transport::GetAverageFilled(MPI_Comm comm, const int rank, const int dst_rank,
                                const bool get_bput, long double *bput,
                                const bool get_bget, long double *bget,
                                const bool get_bgetop, long double *bgetop,
                                const bool get_bdel, long double *bdel) const {
    auto average_filled = [](const Transport::Transport::Stats::Op &op) -> long double {
        std::lock_guard<std::mutex> lock(op.mutex);
        long double sum = 0;
        for(REF(op.filled)::value_type const &filled : op.filled) {
            sum += filled.percent;
        }

        return sum / op.filled.size();
    };

    return GetFilled(comm, rank, dst_rank,
                     get_bput, bput,
                     get_bget, bget,
                     get_bgetop, bgetop,
                     get_bdel, bdel,
                     stats,
                     average_filled);
}

/**
 * GetMaxFilled
 * Collective operation
 * Collects statistics from all ranks in the communicator
 *
 * @param comm        the MPI communicator
 * @param rank        the rank of this instance of Transport
 * @param dst_rank    the rank to send to
 * @param get_bput    whether or not to get bput
 * @param bput        the array of bput from each rank
 * @param get_bget    whether or not to get bget
 * @param bget        the array of bget from each rank
 * @param get_bgetop  whether or not to get bgetop
 * @param bgetop      the array of bgetop from each rank
 * @param get_bdel    whether or not to get bdel
 * @param bdel        the array of bdel from each rank
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
 */
int Transport::GetMaxFilled(MPI_Comm comm, const int rank, const int dst_rank,
                            const bool get_bput, long double *bput,
                            const bool get_bget, long double *bget,
                            const bool get_bgetop, long double *bgetop,
                            const bool get_bdel, long double *bdel) const {
    auto max_filled = [](const Transport::Transport::Stats::Op &op) -> long double {
        std::lock_guard<std::mutex> lock(op.mutex);
        long double max = 0;
        for(REF(op.filled)::value_type const &filled : op.filled) {
            max = std::max(max, filled.percent);
        }

        return max;
    };

    return GetFilled(comm, rank, dst_rank,
                     get_bput, bput,
                     get_bget, bget,
                     get_bgetop, bgetop,
                     get_bdel, bdel,
                     stats,
                     max_filled);
}

}
