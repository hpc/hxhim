#ifndef HXHIM_PROCESS_HPP
#define HXHIM_PROCESS_HPP

#include "hxhim/hxhim.hpp"
#include "hxhim/private.hpp"
#include "hxhim/shuffle.hpp"
#include "hxhim/Results.hpp"

/**
 * process
 * The core set of function calls that are needed to send requests and receive responses
 *
 * @tparam Send_t      the transport request type
 * @tparam Recv_t      the transport response type
 * @tparam UserData_t  unsorted hxhim user data
 * @param hx           the HXHIM session
 * @param head         the head of the list of requests to send
 * @return results from sending requests
 */
template <typename Send_t, typename Recv_t, typename UserData_t>
hxhim::Results *process(hxhim_t *hx,
                        UserData_t *head,
                        const std::size_t max_ops_per_send) {
    mlog(HXHIM_CLIENT_DBG, "Start processing ");

    if (!head) {
        return nullptr;
    }

    // serialized results
    hxhim::Results *res = hx->p->memory_pools.results->acquire<hxhim::Results>(hx);

    // declare local requests here to not reallocate every loop
    Send_t local(hx->p->memory_pools.arrays, hx->p->memory_pools.buffers, max_ops_per_send);
    local.src = hx->p->bootstrap.rank;
    local.dst = hx->p->bootstrap.rank;

    // maximum number of remote destinations allowed at any time
    static const std::size_t max_remote = std::max(hx->p->memory_pools.requests->regions() / 2, (std::size_t) 1);

    // a round might not send every request, so keep running until out of requests
    while (hx->p->running && head) {
        // current set of remote destinations to send to
        std::unordered_map<int, Send_t *> remote;

        // reset local without deallocating memory
        local.count = 0;

        UserData_t *curr = head;

        while (hx->p->running && curr) {
            if (hxhim::shuffle(hx, max_ops_per_send,
                               curr,
                               &local,
                               remote) > -1) {
                // remove the current operation from the list of
                // operations queued up and continue processing

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
                hx->p->memory_pools.ops_cache->release(curr);
                curr = next;
            }
            else {
                curr = curr->next;
            }
        }

        // process remote data
        if (hx->p->running && remote.size()) {
            // hxhim::collect_fill_stats(remote, hx->p->stats.bget);
            Recv_t *responses = hx->p->transport->communicate(remote);
            for(Recv_t *curr = responses; curr; curr = Transport::next(curr, hx->p->memory_pools.responses)) {
                for(std::size_t i = 0; i < curr->count; i++) {
                    res->Add(hxhim::Result::init(hx, curr, i));
                }
            }
        }

        for(typename decltype(remote)::value_type const &dst : remote) {
            hx->p->memory_pools.requests->release(dst.second);
        }

        if (hx->p->running && local.count) {
            hxhim::collect_fill_stats(&local, hx->p->stats.bget);
            Recv_t *responses = local_client(hx, &local);
            for(Recv_t *curr = responses; curr; curr = Transport::next(curr, hx->p->memory_pools.responses)) {
                for(std::size_t i = 0; i < curr->count; i++) {
                    res->Add(hxhim::Result::init(hx, curr, i));
                }
            }
        }
    }

    return res;
}

#endif
