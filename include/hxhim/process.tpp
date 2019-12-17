#ifndef HXHIM_PROCESS_TPP
#define HXHIM_PROCESS_TPP

#include "hxhim/hxhim.hpp"
#include "hxhim/local_client.tpp"
#include "hxhim/private.hpp"
#include "hxhim/shuffle.hpp"
#include "hxhim/Results.hpp"
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
        return nullptr;
    }

    // serialized results
    hxhim::Results *res = construct<hxhim::Results>();

    // declare local requests here to not reallocate every loop
    Request_t local(max_ops_per_send);
    local.src = hx->p->bootstrap.rank;
    local.dst = hx->p->bootstrap.rank;

    // a round might not send every request, so keep running until out of requests
    while (hx->p->running && head) {
        // current set of remote destinations to send to
        std::unordered_map<int, Request_t *> remote;

        // reset local without deallocating memory
        local.count = 0;

        for(UserData_t *curr = head; hx->p->running && curr;) {
            mlog(HXHIM_CLIENT_DBG, "Preparing to shuffle %p (%p)", curr, curr->next);
            if (hxhim::shuffle(hx, max_ops_per_send,
                               curr,
                               &local,
                               remote) > -1) {
                // remove the current operation from the list of
                // operations queued up and continue processing

                mlog(HXHIM_CLIENT_DBG, "Successfully shuffled %p (%p)", curr, curr->next);

                // head node
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

                UserData_t *next = curr->next;

                curr->prev = nullptr;
                curr->next = nullptr;

                // deallocate current node
                destruct(curr);
                curr = next;
            }
            else {
                mlog(HXHIM_CLIENT_DBG, "Failed to shuffle %p (%p)", curr, curr->next);
                curr = curr->next;
            }
        }

        // process remote data
        if (hx->p->running && remote.size()) {
            // hxhim::collect_fill_stats(remote, hx->p->stats.bget);
            Transport::Response::Response *responses = hx->p->transport->communicate(remote);
            for(Transport::Response::Response *curr = responses; curr; curr = Transport::next(curr)) {
                for(std::size_t i = 0; i < curr->count; i++) {
                    res->Add(hxhim::Result::init(hx, curr, i));
                }
            }
        }

        for(typename decltype(remote)::value_type const &dst : remote) {
            destruct(dst.second);
        }

        // process local data
        if (hx->p->running && local.count) {
            hxhim::collect_fill_stats(&local, hx->p->stats.bget);
            Response_t *responses = local_client<Request_t, Response_t>(hx, &local);
            for(Transport::Response::Response *curr = responses; curr; curr = Transport::next(curr)) {
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
