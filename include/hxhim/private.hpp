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

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct hxhim_private {
    struct {
        MPI_Comm comm;
        int rank;
        int size;
    } mpi;

    hxhim::backend::base *backend;

    std::atomic_bool running;

    // unsent data
    hxhim::Unsent<hxhim::PutData> puts;
    hxhim::Unsent<hxhim::GetData> gets;
    hxhim::Unsent<hxhim::GetOpData> getops;
    hxhim::Unsent<hxhim::DeleteData> deletes;

    // asynchronous PUT data
    std::size_t watermark;               // number of batches to hold before sending PUTs asynchronously
    std::thread background_put_thread;
    std::mutex put_results_mutex;
    hxhim::Results *put_results;

} hxhim_private_t;

#ifdef __cplusplus
}
#endif

#endif
