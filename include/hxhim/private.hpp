#ifndef HXHIM_PRIVATE_HPP
#define HXHIM_PRIVATE_HPP

#include <atomic>
#include <mutex>
#include <thread>

#include <mpi.h>

#include "hxhim/Results.hpp"
#include "hxhim/backend/base.hpp"
#include "hxhim/cache.hpp"
#include "hxhim/constants.h"
#include "hxhim/options.h"
#include "hxhim/struct.h"
#include "utils/Histogram.hpp"

typedef struct hxhim_private {
    hxhim_private();

    hxhim::backend::base *backend;

    // unsent data
    hxhim::Unsent<hxhim::PutData> puts;
    hxhim::Unsent<hxhim::GetData> gets;
    hxhim::Unsent<hxhim::GetOpData> getops;
    hxhim::Unsent<hxhim::DeleteData> deletes;

    // asynchronous PUT data
    std::atomic_bool running;
    std::size_t queued_bputs;               // number of batches to hold before sending PUTs asynchronously
    std::thread background_put_thread;
    std::mutex put_results_mutex;
    hxhim::Results *put_results;

    Histogram::Histogram *histogram;

} hxhim_private_t;

namespace hxhim {
int encode(const hxhim_spo_type_t type, void *&ptr, std::size_t &len, bool &copied);

namespace init {
int types(hxhim_t *hx, hxhim_options_t *opts);
int backend(hxhim_t *hx, hxhim_options_t *opts);
int one_backend(hxhim_t *hx, hxhim_options_t *opts, const std::string &name);
int background_thread(hxhim_t *hx, hxhim_options_t *opts);
int histogram(hxhim_t *hx, hxhim_options_t *opts);
}

namespace destroy {
int types(hxhim_t *hx);
int backend(hxhim_t *hx);
int background_thread(hxhim_t *hx);
int histogram(hxhim_t *hx);
}
}

#endif
