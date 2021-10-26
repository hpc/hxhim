#ifndef HXHIM_RESULTS_PRIVATE_HPP
#define HXHIM_RESULTS_PRIVATE_HPP

#include "hxhim/Results.hpp"
#include "hxhim/struct.h"
#include "message/Messages.hpp"

namespace hxhim {
    namespace Result {
        Results::Result *init(hxhim_t *hx, Message::Response::Response *res,     const std::size_t i);
        Results::Put    *init(hxhim_t *hx, Message::Response::BPut *bput,        const std::size_t i);
        Results::Get    *init(hxhim_t *hx, Message::Response::BGet *bget,        const std::size_t i);
        Results::GetOp  *init(hxhim_t *hx, Message::Response::BGetOp *bgetop,    const std::size_t i);
        Results::Delete *init(hxhim_t *hx, Message::Response::BDelete *bdel,     const std::size_t i);
        Results::Sync   *init(hxhim_t *hx, const int synced);
        Results::Hist   *init(hxhim_t *hx, Message::Response::BHistogram *bhist, const std::size_t i);

        // add all responses into results with one call
        void AddAll(hxhim_t *hx, hxhim::Results *results, Message::Response::Response *response);
    }
}

extern "C"
{

typedef struct hxhim_results {
    hxhim_t *hx;
    hxhim::Results *res;
} hxhim_results_t;

}

/* wraps a hxhim::Results with a hxhim_results_t */
hxhim_results_t *hxhim_results_init(hxhim_t *hx, hxhim::Results *res);

#endif
