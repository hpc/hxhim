#ifndef TRANSPORT_BACKEND_LOCAL_PROCESS_TPP
#define TRANSPORT_BACKEND_LOCAL_PROCESS_TPP

#include <cstring>

#include "hxhim/private/Results.hpp"
#include "hxhim/private/accessors.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/private/shuffle.hpp"
#include "transport/backend/local/RangeServer.hpp"
#include "utils/Stats.hpp"
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
    Request_t local(hx->p->max_ops_per_send);
    local.src = local.dst = rank;
    Request_t **requests = alloc_array<Request_t *>(size);

    // a round might not send every request, so keep running until out of requests
    while (head) {
        // reset buffers
        memset(requests, 0, sizeof(Request_t *) * size);
        local.count = 0;
        local.alloc_transport_timestamp();

        // local is placed into remote to make bulking easier
        requests[rank] = &local;

        #if PRINT_TIMESTAMPS
        ::Stats::Chronopoint fill_start = ::Stats::now();
        #endif

        for(UserData_t *curr = head; curr;) {
            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint shuffle_start = ::Stats::now();
            #endif
            mlog(HXHIM_CLIENT_DBG, "Rank %d Client preparing to shuffle %p (next: %p)", rank, curr, curr->next);

            const int dst_ds = hxhim::shuffle::shuffle(curr, hx->p->max_ops_per_send, requests);

            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint shuffle_end = ::Stats::now();
            {
                ::Stats::Chronopoint print_start = ::Stats::now();
                ::Stats::print_event(hx->p->print_buffer, rank, "process_shuffle", ::Stats::global_epoch, shuffle_start, shuffle_end);
                ::Stats::Chronopoint print_end = ::Stats::now();
                ::Stats::print_event(hx->p->print_buffer, rank, "print", ::Stats::global_epoch, print_start, print_end);
            }
            #endif

            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint next_start = ::Stats::now();
            #endif

            UserData_t *next = curr->next;

            // scan packets if current item could not be inserted
            if (dst_ds == hxhim::shuffle::NOSPACE) {

                // if all packets are full, stop processing of the rest of the work queue
                // saves effort if all queues are filled up early
                std::size_t filled = 0;
                std::size_t created = 0;
                for(int i = 0; i < size; i++) {
                    if (requests[i]) {
                        created++;
                        filled += (requests[i]->count == requests[i]->max_count);
                    }
                }

                if (created == filled) {
                    #if PRINT_TIMESTAMPS
                    ::Stats::Chronopoint next_end = ::Stats::now();
                    {
                        ::Stats::Chronopoint print_start = ::Stats::now();
                        ::Stats::print_event(hx->p->print_buffer, rank, "Break", ::Stats::global_epoch, next_start, next_end);
                        ::Stats::Chronopoint print_end = ::Stats::now();
                        ::Stats::print_event(hx->p->print_buffer, rank, "print", ::Stats::global_epoch, print_start, print_end);
                    }
                    #endif
                    break;
                }
            }
            else /* if (dst_ds > -1) */ {
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
            }

            curr = next;

            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint next_end = ::Stats::now();
            {
                ::Stats::Chronopoint print_start = ::Stats::now();
                ::Stats::print_event(hx->p->print_buffer, rank, "Next", ::Stats::global_epoch, next_start, next_end);
                ::Stats::Chronopoint print_end = ::Stats::now();
                ::Stats::print_event(hx->p->print_buffer, rank, "print", ::Stats::global_epoch, print_start, print_end);
            }
            #endif
        }

        #if PRINT_TIMESTAMPS
        ::Stats::Chronopoint fill_end = ::Stats::now();
        ::Stats::print_event(hx->p->print_buffer, rank, "process_fill", ::Stats::global_epoch, fill_start, fill_end);
        #endif

        // extra time that needs to be added to the results duration
        long double extra_time = 0;

        // remove requests destined for local datastore
        requests[rank] = nullptr;

        // move requests into remote map
        Transport::ReqList<Request_t> remote;
        for(int i = 0; i < size; i++) {
            Request_t *req = requests[i];
            if (req) {
                req->src = rank;
                req->dst = i;

                remote[i] = req;

                // collect stats
                ::Stats::Chronopoint collect_stats_start = ::Stats::now();
                hx->p->stats.used[req->op].push_back(req->filled());
                hx->p->stats.outgoing[req->op][req->dst]++;
                ::Stats::Chronopoint collect_stats_end = ::Stats::now();
                #if PRINT_TIMESTAMPS
                ::Stats::print_event(hx->p->print_buffer, rank, "collect_stats",
                                     ::Stats::global_epoch, collect_stats_start, collect_stats_end);
                #endif

                extra_time += ::Stats::sec(collect_stats_start,
                                           collect_stats_end);
            }
        }

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

    dealloc_array(requests);

    return res;
}

}

#endif
