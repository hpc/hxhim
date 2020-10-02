#ifndef TRANSPORT_BACKEND_LOCAL_PROCESS_TPP
#define TRANSPORT_BACKEND_LOCAL_PROCESS_TPP

#include <cstring>

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
 * @return results from sending requests
 */
template <typename UserData_t, typename Request_t, typename Response_t,
          typename = enable_if_t <is_child_of <hxhim::UserData,               UserData_t>::value &&
                                  is_child_of <Transport::Request::Request,   Request_t> ::value &&
                                  is_child_of <Transport::Response::Response, Response_t>::value  >
          >
hxhim::Results *process(hxhim_t *hx,
                        UserData_t *head) {

    int rank = -1;
    int size = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, &size);

    mlog(HXHIM_CLIENT_DBG, "Rank %d Start processing", rank);

    #if PRINT_TIMESTAMPS
    ::Stats::Chronopoint epoch;
    hxhim::nocheck::GetEpoch(hx, epoch);
    #endif

    // serialized results
    hxhim::Results *res = construct<hxhim::Results>(hx);

    // buffers to bulking up user data
    Request_t local;
    local.src = local.dst = rank;
    Request_t **remote_ptrs = alloc_array<Request_t *>(size);

    // a round might not send every request, so keep running until out of requests
    while (head) {
        // reset buffers
        memset(remote_ptrs, 0, sizeof(Request_t *) * size);
        local.alloc(hx->p->max_ops_per_send);

        // local is placed into remote to make bulking easier
        remote_ptrs[rank] = &local;

        #if PRINT_TIMESTAMPS
        ::Stats::Chronopoint fill_start = ::Stats::now();
        #endif

        for(UserData_t *curr = head; curr;) {
            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint shuffle_start = ::Stats::now();
            #endif
            mlog(HXHIM_CLIENT_DBG, "Rank %d Client preparing to shuffle %p (next: %p)", rank, curr, curr->next);

            UserData_t *next = curr->next;
            const int dst_ds = hxhim::shuffle::shuffle(hx, curr, remote_ptrs);
            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint shuffle_end = ::Stats::now();

            ::Stats::Chronopoint print_start = ::Stats::now();
            ::Stats::print_event(hx->p->print_buffer, rank, "process_shuffle", ::Stats::global_epoch, shuffle_start, shuffle_end);
            ::Stats::Chronopoint print_end = ::Stats::now();
            ::Stats::print_event(hx->p->print_buffer, rank, "print", ::Stats::global_epoch, print_start, print_end);
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

            // scan packets if current item could not be inserted
            if (dst_ds == hxhim::shuffle::NOSPACE) {
                #if PRINT_TIMESTAMPS
                ::Stats::Chronopoint break_start = ::Stats::now();
                #endif

                // if all packets are full, stop processing of the rest of the work queue
                // saves effort if all queues are filled up early
                std::size_t filled = 0;
                std::size_t created = 0;
                for(int i = 0; i < size; i++) {
                    if (remote_ptrs[i]) {
                        created++;
                        filled += (remote_ptrs[i]->count == remote_ptrs[i]->max_count);
                    }
                }

                #if PRINT_TIMESTAMPS
                ::Stats::Chronopoint break_end = ::Stats::now();

                ::Stats::Chronopoint print_start = ::Stats::now();
                ::Stats::print_event(hx->p->print_buffer, rank, "Break", ::Stats::global_epoch, break_start, break_end);
                ::Stats::Chronopoint print_end = ::Stats::now();
                ::Stats::print_event(hx->p->print_buffer, rank, "print", ::Stats::global_epoch, print_start, print_end);
                #endif

                if (created == filled) {
                    break;
                }
            }

            curr = next;
        }

        #if PRINT_TIMESTAMPS
        ::Stats::Chronopoint fill_end = ::Stats::now();
        ::Stats::print_event(hx->p->print_buffer, rank, "process_fill", ::Stats::global_epoch, fill_start, fill_end);
        #endif

        // extra time that needs to be added to the results duration
        long double extra_time = 0;

        Transport::ReqList<Request_t> remote;
        for(int i = 0; i < size; i++) {
            if (remote[i]) {
                remote[i] = remote_ptrs[i];

                // collect stats
                ::Stats::Chronopoint collect_stats_start = ::Stats::now();
                hx->p->stats.used[remote[i]->op].push_back(remote[i]->filled());
                hx->p->stats.outgoing[remote[i]->op][remote[i]->dst]++;
                ::Stats::Chronopoint collect_stats_end = ::Stats::now();
                #if PRINT_TIMESTAMPS
                ::Stats::print_event(hx->p->print_buffer, rank, "collect_stats", ::Stats::global_epoch, collect_stats_start, collect_stats_end);
                #endif

                extra_time += ::Stats::sec(collect_stats_start,
                                           collect_stats_end);
            }
        }

        // remove requests destined for local datastore
        remote.erase(rank);

        mlog(HXHIM_CLIENT_DBG, "Rank %d Client packed together requests destined for %zu remote servers", rank, remote.size());

        // process remote data
        if (remote.size()) {
            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint remote_start = ::Stats::now();
            #endif

            // send down transport layer
            Response_t *response = hx->p->transport->communicate(remote);

            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint remote_end = ::Stats::now();
            ::Stats::print_event(hx->p->print_buffer, rank, "remote",
                                 ::Stats::global_epoch, remote_start, remote_end);
            #endif

            // serialize results
            hxhim::Result::AddAll(hx, res, response);
        }

        mlog(HXHIM_CLIENT_DBG, "Rank %d Client sending %zu local requests", rank, local.count);

        // process local data
        if (local.count) {
            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint local_start = ::Stats::now();
            #endif

            // send to local range server
            Response_t *response = Transport::local::range_server<Response_t, Request_t>(hx, &local);

            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint local_end = ::Stats::now();
            ::Stats::print_event(hx->p->print_buffer, rank, "local",
                                 ::Stats::global_epoch, local_start, local_end);
            #endif

            // serialize results
            hxhim::Result::AddAll(hx, res, response);
        }

        res->UpdateDuration(extra_time);

        mlog(HXHIM_CLIENT_DBG, "Rank %d Client received %zu responses", rank, res->Size());
    }
    dealloc_array(remote_ptrs);

    return res;
}

}

#endif
