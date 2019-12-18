#ifndef HXHIM_RESULTS_PRIVATE_HPP
#define HXHIM_RESULTS_PRIVATE_HPP

#include "hxhim/Results.hpp"
#include "hxhim/struct.h"
#include "transport/Messages/Messages.hpp"

namespace hxhim {
    namespace Result {
        Results::Result    *init(hxhim_t *hx, Transport::Response::Response *res, const std::size_t i);
        Results::Put       *init(hxhim_t *hx, Transport::Response::BPut *bput, const std::size_t i);
        Results::Get2      *init(hxhim_t *hx, Transport::Response::BGet2 *bget, const std::size_t i);
        Results::Get2      *init(hxhim_t *hx, Transport::Response::BGetOp *bgetop, const std::size_t i);
        Results::Delete    *init(hxhim_t *hx, Transport::Response::BDelete *bdel, const std::size_t i);
        Results::Sync      *init(hxhim_t *hx, const int ds_offset, const int synced);
        Results::Histogram *init(hxhim_t *hx, Transport::Response::BHistogram *bhist, const std::size_t i);
    }
}

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct hxhim_results {
    hxhim::Results *res;
} hxhim_results_t;

#ifdef __cplusplus
}
#endif

hxhim_results_t *hxhim_results_init(hxhim::Results *res);

#endif
