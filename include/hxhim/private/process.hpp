#ifndef TRANSPORT_BACKEND_LOCAL_PROCESS_HPP
#define TRANSPORT_BACKEND_LOCAL_PROCESS_HPP

#include <cstring>

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
    const int rank = hx->p->bootstrap.rank;
    #endif

    // serialized results
    hxhim::Results *res = construct<hxhim::Results>();
    uint64_t extra_time = 0;

    while (remaining(queue)) {
        // extract the first packet for each target range server
        Request_t *local = nullptr;
        Transport::ReqList<Request_t> remote;
        for(std::size_t i = 0; i < queue.size(); i++) {
            if (queue[i].size()) {
                Request_t *req = queue[i].front();
                queue[i].pop_front();

                req->src = hx->p->bootstrap.rank;
                req->dst = hx->p->queues.rs_to_rank[i];

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

            ::Stats::Chronopoint destruct_start = ::Stats::now();
            for(REF(remote)::value_type const &req : remote) {
                destruct(req.second);
            }
            ::Stats::Chronopoint destruct_end = ::Stats::now();
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

            // serialize results
            hxhim::Result::AddAll(hx, res, response);

            ::Stats::Chronopoint destruct_start = ::Stats::now();
            destruct(local);
            ::Stats::Chronopoint destruct_end = ::Stats::now();
            extra_time += ::Stats::nano(destruct_start, destruct_end);
        }
    }

    res->UpdateDuration(extra_time);

    return res;
}

}

#endif
