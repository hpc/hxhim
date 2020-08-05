#ifndef HXHIM_RESULTS_PRIVATE_HPP
#define HXHIM_RESULTS_PRIVATE_HPP

#include "hxhim/Results.hpp"
#include "hxhim/struct.h"
#include "transport/Messages/Messages.hpp"

namespace hxhim {
    namespace Result {
        Results::Result *init(hxhim_t *hx, Transport::Response::Response *res,     const std::size_t i);
        Results::Put    *init(hxhim_t *hx, Transport::Response::BPut *bput,        const std::size_t i);
        Results::Get    *init(hxhim_t *hx, Transport::Response::BGet *bget,        const std::size_t i);
        Results::GetOp  *init(hxhim_t *hx, Transport::Response::BGetOp *bgetop,    const std::size_t i);
        Results::Delete *init(hxhim_t *hx, Transport::Response::BDelete *bdel,     const std::size_t i);
        Results::Sync   *init(hxhim_t *hx, const int ds_offset, const int synced);
        Results::Hist   *init(hxhim_t *hx, Transport::Response::BHistogram *bhist, const std::size_t i);

        // add all responses into results with one call
        uint64_t AddAll(hxhim_t *hx, hxhim::Results *results, Transport::Response::Response *response);
    }
}

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct hxhim_results {
    hxhim_t *hx;
    hxhim::Results *res;
} hxhim_results_t;

#ifdef __cplusplus
}
#endif

/* wraps a hxhim::Results with a hxhim_results_t */
hxhim_results_t *hxhim_results_init(hxhim_t *hx, hxhim::Results *res);

#endif
