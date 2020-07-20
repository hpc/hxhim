#ifndef TRANSPORT_BACKEND_LOCAL_PROCESS_TPP
#define TRANSPORT_BACKEND_LOCAL_PROCESS_TPP

#include <unordered_map>

#include "hxhim/private/Results.hpp"
#include "hxhim/private/accessors.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/private/shuffle.hpp"
#include "transport/backend/local/RangeServer.hpp"
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

    mlog(HXHIM_CLIENT_DBG, "Rank %d Start processing", rank);

    if (!head) {
        mlog(HXHIM_CLIENT_WARN, "Rank %d Nothing to process", rank);
        return nullptr;
    }

    // serialized results
    hxhim::Results *res = construct<hxhim::Results>(hx);

    // declare local requests here to not reallocate every loop
    Request_t local(max_ops_per_send);
    local.src = rank;
    local.dst = rank;

    // a round might not send every request, so keep running until out of requests
    while (head) {
        // current set of remote destinations to send to
        std::unordered_map<int, Request_t *> remote;

        // reset local without deallocating memory
        local.count = 0;

        for(UserData_t *curr = head; curr;) {
            mlog(HXHIM_CLIENT_DBG, "Rank %d Client preparing to shuffle %p (next: %p)", rank, curr, curr->next);

            UserData_t *next = curr->next;
            const int dst_ds = hxhim::shuffle::shuffle(hx, rank, max_ops_per_send, curr, &local, remote);

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

            curr = next;
        }

        mlog(HXHIM_CLIENT_DBG, "Rank %d Client packed together requests destined for %zu remote servers", rank, remote.size());

        // process remote data
        if (remote.size()) {
            // collect stats
            for(typename decltype(remote)::const_iterator it = remote.begin();
                it != remote.end(); it++) {
                hx->p->stats.used[it->second->type].push_back(it->second->filled());
                hx->p->stats.outgoing[it->second->type][it->second->dst]++;
            }

            Transport::Response::Response *responses = hx->p->transport->communicate(remote);
            timespec end;
            clock_gettime(CLOCK_MONOTONIC, &end);

            for(Transport::Response::Response *curr = responses; curr; curr = next(curr)) {
                for(std::size_t i = 0; i < curr->count; i++) {
                    res->Add(hxhim::Result::init(hx, curr, i));
                }
            }
        }

        for(REF(remote)::value_type &dst : remote) {
            destruct(dst.second);
        }

        mlog(HXHIM_CLIENT_DBG, "Rank %d Client sending %zu local requests", rank, local.count);

        // process local data
        if (local.count) {
            // collect stats
            hx->p->stats.used[local.type].push_back(local.filled());
            hx->p->stats.outgoing[local.type][local.dst]++;

            Response_t *responses = Transport::local::range_server<Response_t, Request_t>(hx, &local);
            for(Transport::Response::Response *curr = responses; curr; curr = next(curr)) {
                for(std::size_t i = 0; i < curr->count; i++) {
                    res->Add(hxhim::Result::init(hx, curr, i));
                }
            }
        }

        mlog(HXHIM_CLIENT_DBG, "Rank %d Client received %zu responses", rank, res->Size());
    }

    return res;
}

}

#endif
