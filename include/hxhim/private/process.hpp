#ifndef TRANSPORT_BACKEND_LOCAL_PROCESS_HPP
#define TRANSPORT_BACKEND_LOCAL_PROCESS_HPP

#include "hxhim/private/Results.hpp"
#include "hxhim/private/hxhim.hpp"
#include "transport/backend/local/RangeServer.hpp"
#include "utils/Stats.hpp"
#include "utils/macros.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"
#include "utils/type_traits.hpp"

namespace hxhim {

template <typename Request_t,
          typename = enable_if_t <is_child_of <Transport::Request::Request, Request_t>::value> >
std::size_t remaining(hxhim::Queue<Request_t> &queue) {
    std::size_t count = 0;
    for(std::size_t i = 0; i < queue.size(); i++) {
        count += queue[i].size();
    }

    return count;
}

template <typename Request_t,
          typename Response_t,
          typename = enable_if_t <is_child_of <Transport::Request::Request,   Request_t>::value  &&
                                  is_child_of <Transport::Response::Response, Response_t>::value> >
hxhim::Results *process(hxhim_t *hx,
                        hxhim::Queue<Request_t> &queue) {
    #if PRINT_TIMESTAMPS
    ::Stats::Chronopoint process_start = ::Stats::now();
    #endif

    #if PRINT_TIMESTAMPS
    const int rank = hx->p->bootstrap.rank;
    #endif

    // serialized results
    hxhim::Results *res = construct<hxhim::Results>();
    uint64_t extra_time = 0;

    while (remaining(queue)) {
        #if PRINT_TIMESTAMPS
        ::Stats::Chronopoint pop_start = ::Stats::now();
        #endif

        // extract the first packet for each target range server
        Request_t *local = nullptr;
        Transport::ReqList<Request_t> remote;
        for(std::size_t rs = 0; rs < queue.size(); rs++) {
            if (queue[rs].size()) {
                Request_t *req = queue[rs].front();
                queue[rs].pop_front();

                // set req src and dst because they were not set in impl
                req->src = hx->p->bootstrap.rank;
                req->dst = hx->p->queues.rs_to_rank[rs];

                if (req->src == req->dst) {
                    local = req;
                }
                else {
                    remote[req->dst] = req;
                }

                ::Stats::Chronopoint collect_stats_start = ::Stats::now();
                hx->p->stats.used[req->op].push_back(req->filled());
                hx->p->stats.outgoing[req->op][req->dst]++;
                ::Stats::Chronopoint collect_stats_end = ::Stats::now();
                #if PRINT_TIMESTAMPS
                ::Stats::print_event(hx->p->print_buffer, rank, "collect_stats",
                                     ::Stats::global_epoch, collect_stats_start, collect_stats_end);
                #endif

                extra_time += ::Stats::nano(collect_stats_start,
                                            collect_stats_end);

            }
        }

        #if PRINT_TIMESTAMPS
        ::Stats::Chronopoint pop_end = ::Stats::now();
        ::Stats::print_event(hx->p->print_buffer, rank, "pop",
                             ::Stats::global_epoch, pop_start, pop_end);
        #endif

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

            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint serialize_start = ::Stats::now();
            #endif

            // serialize results
            hxhim::Result::AddAll(hx, res, response);

            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint serialize_end = ::Stats::now();
            ::Stats::print_event(hx->p->print_buffer, rank, "serialize",
                                 ::Stats::global_epoch, serialize_start, serialize_end);
            #endif

            ::Stats::Chronopoint destruct_start = ::Stats::now();
            for(REF(remote)::value_type const &req : remote) {
                destruct(req.second);
            }
            ::Stats::Chronopoint destruct_end = ::Stats::now();

            #if PRINT_TIMESTAMPS
            ::Stats::print_event(hx->p->print_buffer, rank, "destruct",
                                 ::Stats::global_epoch, destruct_start, destruct_end);
            #endif

            extra_time += ::Stats::nano(destruct_start, destruct_end);
        }

        // process local data
        if (local) {
            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint local_start = ::Stats::now();
            #endif

            // send to local range server
            Response_t *response = Transport::local::range_server<Response_t, Request_t>(hx, local);

            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint local_end = ::Stats::now();
            ::Stats::print_event(hx->p->print_buffer, rank, "local",
                                 ::Stats::global_epoch, local_start, local_end);
            #endif

            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint serialize_start = ::Stats::now();
            #endif

            // serialize results
            hxhim::Result::AddAll(hx, res, response);

            #if PRINT_TIMESTAMPS
            ::Stats::Chronopoint serialize_end = ::Stats::now();
            ::Stats::print_event(hx->p->print_buffer, rank, "serialize",
                                 ::Stats::global_epoch, serialize_start, serialize_end);
            #endif

            ::Stats::Chronopoint destruct_start = ::Stats::now();
            destruct(local);
            ::Stats::Chronopoint destruct_end = ::Stats::now();

            #if PRINT_TIMESTAMPS
            ::Stats::print_event(hx->p->print_buffer, rank, "destruct",
                                 ::Stats::global_epoch, destruct_start, destruct_end);
            #endif

            extra_time += ::Stats::nano(destruct_start, destruct_end);
        }
    }

    res->UpdateDuration(extra_time);

    #if PRINT_TIMESTAMPS
    ::Stats::Chronopoint process_end = ::Stats::now();
    ::Stats::print_event(hx->p->print_buffer, rank, "process",
        ::Stats::global_epoch, process_start, process_end);
    #endif

    return res;
}

}

#endif
