#ifndef TRANSPORT_BACKEND_LOCAL_PROCESS_TPP
#define TRANSPORT_BACKEND_LOCAL_PROCESS_TPP

#include <unordered_map>

#include "hxhim/private/Results.hpp"
#include "hxhim/private/accessors.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/private/shuffle.hpp"
#include "transport/backend/local/RangeServer.hpp"
#include "utils/Stats.hpp"
#include "utils/is_range_server.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"
#include "utils/type_traits.hpp"

namespace hxhim {

/**
 * process
 * The core set of function calls that are needed to send requests and receive responses
 * Converts a UserData_t into Request_ts for transport
 * The Request_ts are converted to Response_ts upon completion of the operation and returned
 *
 * @tparam UserData_t      unsorted hxhim user data type
 * @tparam Response_t      Transport::Response::*
 * @tparam Request_t       Transport::Request::*
 * @param hx               the HXHIM session
 * @param head             the head of the list of requests to send
 * @param max_ops_per_send the maximum number of sets of data that can be processed in a single packet
 * @return results from sending requests
 */
template <typename UserData_t, typename Request_t, typename Response_t,
          typename = enable_if_t <is_child_of <hxhim::UserData,               UserData_t>::value &&
                                  is_child_of <Transport::Request::Request,   Request_t> ::value &&
                                  is_child_of <Transport::Response::Response, Response_t>::value  >
          >
hxhim::Results *process(hxhim_t *hx,
                        UserData_t *head,
                        const std::size_t max_ops_per_send) {
    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    #if PRINT_TIMESTAMPS
    ::Stats::Chronopoint epoch;
    hxhim::nocheck::GetEpoch(hx, epoch);
    #endif

    mlog(HXHIM_CLIENT_DBG, "Rank %d Start processing", rank);

    // serialized results
    hxhim::Results *res = construct<hxhim::Results>(hx);

    // a round might not send every request, so keep running until out of requests
    while (head) {
        // current set of remote destinations to send to
        Transport::ReqList<Request_t> remote;

        Request_t local(max_ops_per_send);
        local.src = local.dst = rank;

        #if PRINT_TIMESTAMPS
        ::Stats::Chronopoint fill_start = ::Stats::now();
        #endif

        for(UserData_t *curr = head; curr;) {
            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint shuffle_start = ::Stats::now();
            #endif
            mlog(HXHIM_CLIENT_DBG, "Rank %d Client preparing to shuffle %p (next: %p)", rank, curr, curr->next);

            UserData_t *next = curr->next;
            const int dst_ds = hxhim::shuffle::shuffle(hx, rank, max_ops_per_send, curr, &local, remote);
            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint shuffle_end = ::Stats::now();
            #endif

            switch (dst_ds) {
                case hxhim::shuffle::NOSPACE:
                    // go to the next request; will come back later
                    break;
                case hxhim::shuffle::ERROR:
                default:
                    // remove the current request from the list and destroy it

                    // remove the current operation from the list of
                    // operations queued up and continue processing

                    // replace head node, since it is about to be destructed
                    if (curr == head) {
                        head = curr->next;
                    }

                    // there is a node before the current one
                    if (curr->prev) {
                        curr->prev->next = curr->next;
                    }

                    // there is a node after the current one
                    if (curr->next) {
                        curr->next->prev = curr->prev;
                    }

                    // deallocate current node
                    curr->prev = nullptr;
                    curr->next = nullptr;
                    destruct(curr);

                    break;
            }

            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint print_start = ::Stats::now();

            ::Stats::print_event(hx->p->print_buffer, rank, "process_shuffle", ::Stats::global_epoch, shuffle_start, shuffle_end);
            ::Stats::Chronopoint print_end = ::Stats::now();
            ::Stats::print_event(hx->p->print_buffer, rank, "print", ::Stats::global_epoch, print_start, print_end);
            #endif

            if (dst_ds == hxhim::shuffle::NOSPACE) {
                // quick scan of all packets
                // if all packets are full, stop processing of the rest of the work queue
                // saves effort if all queues are filled up early
                std::size_t filled = (local.count == local.max_count);
                for(typename decltype(remote)::value_type const & rem : remote) {
                    filled += (rem.second->count == rem.second->max_count);
                }

                if (filled == (remote.size() + 1)) {
                    break;
                }
            }

            curr = next;
        }

        #if PRINT_TIMESTAMPS
        ::Stats::Chronopoint fill_end = ::Stats::now();
        ::Stats::print_event(hx->p->print_buffer, rank, "process_fill", ::Stats::global_epoch, fill_start, fill_end);
        #endif

        mlog(HXHIM_CLIENT_DBG, "Rank %d Client packed together requests destined for %zu remote servers", rank, remote.size());

        // extra time that needs to be added to the results duration
        long double extra_time = 0;

        // process remote data
        if (remote.size()) {
            // collect stats
            ::Stats::Chronopoint collect_stats_start = ::Stats::now();
            for(REF(remote)::value_type &dst : remote) {
                hx->p->stats.used[dst.second->op].push_back(dst.second->filled());
                hx->p->stats.outgoing[dst.second->op][dst.second->dst]++;
            }
            ::Stats::Chronopoint collect_stats_end = ::Stats::now();
            #if PRINT_TIMESTAMPS
            ::Stats::print_event(hx->p->print_buffer, rank, "collect_stats", ::Stats::global_epoch, collect_stats_start, collect_stats_end);
            #endif

            extra_time += ::Stats::sec(collect_stats_start,
                                       collect_stats_end);

            // send down transport layer
            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint remote_start = ::Stats::now();
            #endif
            Response_t *response = hx->p->transport->communicate(remote);
            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint remote_end = ::Stats::now();
            ::Stats::print_event(hx->p->print_buffer, rank, "remote", ::Stats::global_epoch, remote_start, remote_end);
            #endif

            // serialize results
            hxhim::Result::AddAll(hx, res, response);
        }

        mlog(HXHIM_CLIENT_DBG, "Rank %d Client sending %zu local requests", rank, local.count);

        // process local data
        if (local.count) {
            // collect stats
            ::Stats::Chronopoint collect_stats_start = ::Stats::now();
            hx->p->stats.used[local.op].push_back(local.filled());
            hx->p->stats.outgoing[local.op][local.dst]++;
            ::Stats::Chronopoint collect_stats_end = ::Stats::now();
            #if PRINT_TIMESTAMPS
            ::Stats::print_event(hx->p->print_buffer, rank, "collect_stats", ::Stats::global_epoch, collect_stats_start, collect_stats_end);
            #endif

            extra_time += ::Stats::sec(collect_stats_start,
                                       collect_stats_end);

            // send to local range server
            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint local_start = ::Stats::now();
            #endif
            Response_t *response = Transport::local::range_server<Response_t, Request_t>(hx, &local);

            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint local_end = ::Stats::now();
            ::Stats::print_event(hx->p->print_buffer, rank, "local", ::Stats::global_epoch, local_start, local_end);
            #endif

            // serialize results
            hxhim::Result::AddAll(hx, res, response);
        }

        res->UpdateDuration(extra_time);

        mlog(HXHIM_CLIENT_DBG, "Rank %d Client received %zu responses", rank, res->Size());
    }

    return res;
}

}

#endif
