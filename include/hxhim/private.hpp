#ifndef HXHIM_PRIVATE_HPP
#define HXHIM_PRIVATE_HPP

#include <atomic>
#include <mutex>
#include <thread>

#include <mpi.h>

#include "Results.hpp"
#include "backend/base.hpp"
#include "cache.hpp"
#include "constants.h"
#include "struct.h"
#include "utils/Histogram.hpp"

typedef struct hxhim_private {
    hxhim_private();

    hxhim_spo_type_t subject_type;
    hxhim_spo_type_t predicate_type;
    hxhim_spo_type_t object_type;

    hxhim::backend::base *backend;

    std::atomic_bool running;

    // unsent data
    hxhim::Unsent<hxhim::PutData> puts;
    hxhim::Unsent<hxhim::GetData> gets;
    hxhim::Unsent<hxhim::GetOpData> getops;
    hxhim::Unsent<hxhim::DeleteData> deletes;

    // asynchronous PUT data
    std::size_t queued_bputs;               // number of batches to hold before sending PUTs asynchronously
    std::thread background_put_thread;
    std::mutex put_results_mutex;
    hxhim::Results *put_results;

    Histogram::Histogram *histogram;

} hxhim_private_t;

#endif
