#ifndef TRANSPORT_BACKEND_LOCAL_PROCESS_TPP
#define TRANSPORT_BACKEND_LOCAL_PROCESS_TPP

#include <unordered_map>

#include "hxhim/Results_private.hpp"
#include "hxhim/private.hpp"
#include "hxhim/shuffle.hpp"
#include "transport/backend/local/RangeServer.hpp"
#include "utils/is_range_server.hpp"
#include "utils/memory.hpp"
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
    mlog(HXHIM_CLIENT_DBG, "Start processing");

    if (!head) {
        mlog(HXHIM_CLIENT_WARN, "Nothing to process");
        return nullptr;
    }

    if (!hx->p->running) {
        mlog(HXHIM_CLIENT_CRIT, "HXHIM has stopped. Not processing requests.");
        return nullptr;
    }

    // serialized results
    hxhim::Results *res = construct<hxhim::Results>();

    // declare local requests here to not reallocate every loop
    Request_t local(max_ops_per_send);
    local.src = hx->p->bootstrap.rank;
    local.dst = hx->p->bootstrap.rank;

    // a round might not send every request, so keep running until out of requests
    while (head) {
        // current set of remote destinations to send to
        std::unordered_map<int, Request_t *> remote;

        // reset local without deallocating memory
        local.count = 0;

        for(UserData_t *curr = head; hx->p->running && curr;) {
            mlog(HXHIM_CLIENT_DBG, "Preparing to shuffle %p (%p)", curr, curr->next);

            UserData_t *next = curr->next;
            const int dst_ds = hxhim::shuffle::shuffle(hx, max_ops_per_send, curr, &local, remote);
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

        // process remote data
        if (remote.size()) {
            hxhim::collect_fill_stats(remote, hx->p->stats.bget);
            Transport::Response::Response *responses = hx->p->transport->communicate(remote);
            for(Transport::Response::Response *curr = responses; curr; curr = next(curr)) {
                for(std::size_t i = 0; i < curr->count; i++) {
                    res->Add(hxhim::Result::init(hx, curr, i));
                }
            }
        }

        for(REF(remote)::value_type &dst : remote) {
            destruct(dst.second);
        }

        // process local data
        if (local.count) {
            hxhim::collect_fill_stats(&local, hx->p->stats.bget);
            Response_t *responses = Transport::local::range_server<Response_t, Request_t>(hx, &local);
            for(Transport::Response::Response *curr = responses; curr; curr = next(curr)) {
                for(std::size_t i = 0; i < curr->count; i++) {
                    res->Add(hxhim::Result::init(hx, curr, i));
                }
            }
        }
    }

    return res;
}

}

#endif
