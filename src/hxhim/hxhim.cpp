#include "hxhim/hxhim.hpp"
#include "hxhim/private/Results.hpp"
#include "hxhim/private/accessors.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/private/options.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

const char *HXHIM_PUT_COMBINATION_STR[] = {
    HXHIM_PUT_COMBINATION_GEN(HXHIM_PUT_COMBINATION_PREFIX, GENERATE_STR)
};

const HXHIM_PUT_COMBINATION HXHIM_PUT_COMBINATIONS_ENABLED[] = {
    HXHIM_PUT_COMBINATION_SPO,

    #if SOP
    HXHIM_PUT_COMBINATION_SOP,
    #endif

    #if PSO
    HXHIM_PUT_COMBINATION_PSO,
    #endif

    #if POS
    HXHIM_PUT_COMBINATION_POS,
    #endif

    #if OSP
    HXHIM_PUT_COMBINATION_OSP,
    #endif

    #if OPS
    HXHIM_PUT_COMBINATION_OPS,
    #endif
};

int hxhim_put_combination_enabled(const HXHIM_PUT_COMBINATION combo) {
    for(std::size_t i = 0; i < HXHIM_PUT_MULTIPLIER; i++) {
        if (HXHIM_PUT_COMBINATIONS_ENABLED[i] == combo) {
            return 1;
        }
    }
    return 0;
};

const size_t HXHIM_PUT_MULTIPLIER = sizeof(HXHIM_PUT_COMBINATIONS_ENABLED) / sizeof(HXHIM_PUT_COMBINATION_SPO);

const char *HXHIM_OP_STR[] = {
    HXHIM_OP_GEN(HXHIM_OP_PREFIX, GENERATE_STR)
};

const char *HXHIM_GETOP_STR[] = {
    HXHIM_GETOP_GEN(HXHIM_GETOP_PREFIX, GENERATE_STR)
};

const char *HXHIM_OBJECT_TYPE_STR[] = {
    HXHIM_OBJECT_TYPE_GEN(HXHIM_OBJECT_TYPE_PREFIX, GENERATE_STR)
};

/**
 * Open
 * Start a HXHIM session
 *
 * @param hx   the HXHIM session
 * @param opts the HXHIM options to use
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Open(hxhim_t *hx, hxhim_options_t *opts) {
    if (!hx || !valid(opts)) {
        return HXHIM_ERROR;
    }

    #if PRINT_TIMESTAMPS
    ::Stats::Chronostamp init;
    init.start = ::Stats::now();
    #endif

    //Open mlog - stolen from plfs
    mlog_open((char *) "hxhim", 0, opts->p->debug_level, opts->p->debug_level, nullptr, 0, MLOG_LOGPID, 0);

    mlog(HXHIM_CLIENT_INFO, "Initializing HXHIM");

    hx->p = new hxhim_private_t();

    if ((init::bootstrap(hx, opts) != HXHIM_SUCCESS) ||
        (init::running  (hx, opts) != HXHIM_SUCCESS) ||
        (init::memory   (hx, opts) != HXHIM_SUCCESS) ||
        (init::hash     (hx, opts) != HXHIM_SUCCESS) ||
        (init::datastore(hx, opts) != HXHIM_SUCCESS) ||
        (init::async_put(hx, opts) != HXHIM_SUCCESS) ||
        (init::transport(hx, opts) != HXHIM_SUCCESS)) {
        MPI_Barrier(hx->p->bootstrap.comm);
        Close(hx);
        mlog(HXHIM_CLIENT_ERR, "Failed to initialize HXHIM");
        return HXHIM_ERROR;
    }

    mlog(HXHIM_CLIENT_INFO, "Waiting for everyone to complete initialization");
    MPI_Barrier(hx->p->bootstrap.comm);
    mlog(HXHIM_CLIENT_INFO, "Successfully initialized HXHIM on rank %d/%d", hx->p->bootstrap.rank, hx->p->bootstrap.size);
    #if PRINT_TIMESTAMPS
    init.end = ::Stats::now();
    ::Stats::print_event(std::cerr, hx->p->bootstrap.rank, "Open",      ::Stats::global_epoch, init);
    #endif
    return HXHIM_SUCCESS;
}

/**
 * hxhimOpen
 * Start a HXHIM session
 *
 * @param hx             the HXHIM session
 * @param bootstrap_comm the MPI communicator used to boostrap MDHIM
 * @param filename       the name of the file to open (for now, it is only the mdhim configuration)
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimOpen(hxhim_t *hx, hxhim_options_t *opts) {
    return hxhim::Open(hx, opts);
}

/**
 * OpenOne
 * Starts a HXHIM session with only 1 backend datastore.
 * This can only be called when the world size is 1.
 *
 * @param hx       the HXHIM session
 * @param opts     the HXHIM options to use
 * @param db_path  the name of the datastore to pass
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::OpenOne(hxhim_t *hx, hxhim_options_t *opts, const std::string &db_path) {
    if (!hx || !valid(opts)) {
        return HXHIM_ERROR;
    }

    //Open mlog - stolen from plfs
    mlog_open((char *) "hxhim", 0, opts->p->debug_level, opts->p->debug_level, nullptr, 0, MLOG_LOGPID, 0);

    hx->p = new hxhim_private_t();

    if ((init::bootstrap     (hx, opts)          != HXHIM_SUCCESS) ||
        (hx->p->bootstrap.size                   != 1)             ||    // Only allow for 1 rank
        (init::running       (hx, opts)          != HXHIM_SUCCESS) ||
        (init::memory        (hx, opts)          != HXHIM_SUCCESS) ||
        // (init::hash          (hx, opts)          != HXHIM_SUCCESS) || // hash is ignored
        (init::one_datastore (hx, opts, db_path) != HXHIM_SUCCESS) ||
        (init::async_put     (hx, opts)          != HXHIM_SUCCESS)) {
        MPI_Barrier(hx->p->bootstrap.comm);
        Close(hx);
        mlog(HXHIM_CLIENT_ERR, "Failed to initialize HXHIM");
        return HXHIM_ERROR;
    }

    mlog(HXHIM_CLIENT_INFO, "Successfully initialized HXHIM");
    MPI_Barrier(hx->p->bootstrap.comm);
    return HXHIM_SUCCESS;
}

/**
 * OpenOne
 * Starts a HXHIM session with only 1 backend datastore
 *
 * @param hx       the HXHIM session
 * @param opts     the HXHIM options to use
 * @param db_path  the name of the datastore to pass
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimOpenOne(hxhim_t *hx, hxhim_options_t *opts, const char *db_path, const size_t db_path_len) {
    return hxhim::OpenOne(hx, opts, std::string(db_path, db_path_len));
}

/**
 * Close
 * Terminates a HXHIM session
 *
 * @param hx the HXHIM session
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Close(hxhim_t *hx) {
    if (!valid(hx)) {
        mlog(HXHIM_CLIENT_ERR, "Bad HXHIM instance");
        return HXHIM_ERROR;
    }

    MPI_Comm comm = MPI_COMM_NULL;
    int rank = -1;
    int size = -1;
    hxhim::nocheck::GetMPI(hx, &comm, &rank, &size);

    mlog(HXHIM_CLIENT_INFO, "Rank %d Starting to shutdown HXHIM", rank);

    mlog(HXHIM_CLIENT_DBG, "Rank %d No longer accepting user input", rank);
    destroy::running(hx);

    mlog(HXHIM_CLIENT_DBG, "Rank %d Waiting for all ranks to complete syncing", rank);
    Results::Destroy(hxhim::Sync(hx));
    MPI_Barrier(comm);
    mlog(HXHIM_CLIENT_DBG, "Rank %d Closing HXHIM", rank);

    destroy::async_put(hx);
    destroy::transport(hx);
    destroy::hash(hx);
    destroy::datastore(hx);
    destroy::memory(hx);

    // stats should not be modified any more

    // print stats here for now
    for(int i = 0; i < size; i++) {
        MPI_Barrier(comm);
        if (rank == i) {
            std::stringstream s;
            print_stats(hx, s);
            mlog(HXHIM_CLIENT_NOTE, "Rank %d Stats\n%s", rank, s.str().c_str());
        }
    }

    destroy::bootstrap(hx);

    // clean up pointer to private data
    delete hx->p;
    hx->p = nullptr;

    mlog(HXHIM_CLIENT_INFO, "Rank %d HXHIM has been shutdown", rank);
    mlog_close();
    return HXHIM_SUCCESS;
}

/**
 * hxhimClose
 * Terminates a HXHIM session
 *
 * @param hx the HXHIM session
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimClose(hxhim_t *hx) {
    return hxhim::Close(hx);
}

/**
 * ChangeHash
 * Changes the hash function and associated
 * datastores.
 *     - This function is a collective.
 *     - Previously queued operations are flushed
 *     - The datastores are synced before the hash is switched.
 *
 * @param hx   the HXHIM session
 * @param name the name of the new hash function
 * @param func the new hash function
 * @param args the extra args that are used in the hash function
 * @return A list of results
 */
hxhim::Results *hxhim::ChangeHash(hxhim_t *hx, const char *name, hxhim_hash_t func, void *args) {
    if (!hxhim::valid(hx)) {
        return nullptr;
    }

    hxhim::Results *res = hxhim::Sync(hx);

    MPI_Barrier(hx->p->bootstrap.comm);

    // change hashes
    hx->p->hash.func = func;
    hx->p->hash.args = args;

    // change datastores
    for(std::size_t i = 0; i < hx->p->datastores.size(); i++) {
        std::stringstream s;
        s << name << "-" << hxhim::datastore::get_id(hx, hx->p->bootstrap.rank, i);
        hx->p->datastores[i]->Open(s.str());
    }

    MPI_Barrier(hx->p->bootstrap.comm);

    return res;
}

/**
 * ChangeHash
 * Changes the hash function and associated
 * datastores.
 *     - This function is a collective.
 *     - Previously queued operations are flushed
 *     - The datastores are synced before the hash is switched.
 *
 * @param hx   the HXHIM session
 * @param name the name of the new hash function
 * @param func the new hash function
 * @param args the extra args that are used in the hash function
 * @return A list of results
 */
hxhim_results_t *hxhimChangeHash(hxhim_t *hx, const char *name, hxhim_hash_t func, void *args) {
    return hxhim_results_init(hx, hxhim::ChangeHash(hx, name, func, args));
}

/**
 * GetStats
 * Collective operation
 * Each desired pointer should be preallocated with space for md->size values
 *
 * @param hx             the HXHIM session
 * @param dst_rank       the rank that is collecting the data
 * @param put_times      the array of put times from each rank
 * @param num_puts       the array of number of puts from each rank
 * @param get_times      the array of get times from each rank
 * @param num_gets       the array of number of gets from each rank
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::GetStats(hxhim_t *hx, const int dst_rank,
                    uint64_t    *put_times,
                    std::size_t *num_puts,
                    uint64_t    *get_times,
                    std::size_t *num_gets) {
    if (!hxhim::valid(hx)) {
        return HXHIM_ERROR;
    }

    const static std::size_t size_size = sizeof(std::size_t);

    // collect from all datastores first
    const std::size_t count = hx->p->datastores.size();
    uint64_t    *local_put_times = alloc_array<uint64_t>    (count);
    std::size_t *local_num_puts  = alloc_array<std::size_t> (count);
    uint64_t    *local_get_times = alloc_array<uint64_t>    (count);
    std::size_t *local_num_gets  = alloc_array<std::size_t> (count);

    auto cleanup = [local_put_times, local_num_puts,
                    local_get_times, local_num_gets,
                    count] () -> void {
        dealloc_array(local_put_times, count);
        dealloc_array(local_num_puts, count);
        dealloc_array(local_get_times, count);
        dealloc_array(local_num_gets, count);
    };

    for(std::size_t i = 0; i < count; i++) {
        if (hx->p->datastores[i]->GetStats(&local_put_times[i],
                                           &local_num_puts[i],
                                           &local_get_times[i],
                                           &local_num_gets[i]) != DATASTORE_SUCCESS) {
            cleanup();
            return HXHIM_ERROR;
        }
    }

    // send to destination rank
    MPI_Comm comm = hx->p->bootstrap.comm;
    MPI_Barrier(comm);

    if (put_times) {
        if (MPI_Gather(local_put_times, count, MPI_UINT64_T,
                             put_times, count, MPI_UINT64_T, dst_rank, comm) != MPI_SUCCESS) {
            cleanup();
            return HXHIM_ERROR;
        }
    }

    if (num_puts) {
        if (MPI_Gather(local_num_puts, size_size * count, MPI_CHAR,
                             num_puts, size_size * count, MPI_CHAR, dst_rank, comm) != MPI_SUCCESS) {
            cleanup();
            return HXHIM_ERROR;
        }
    }

    if (get_times) {
        if (MPI_Gather(local_get_times, count, MPI_UINT64_T,
                             get_times, count, MPI_UINT64_T, dst_rank, comm) != MPI_SUCCESS) {
            cleanup();
            return HXHIM_ERROR;
        }
    }

    if (num_gets) {
        if (MPI_Gather(local_num_gets, size_size * count, MPI_CHAR,
                             num_gets, size_size * count, MPI_CHAR, dst_rank, comm) != MPI_SUCCESS) {
            cleanup();
            return HXHIM_ERROR;
        }
    }

    MPI_Barrier(comm);
    cleanup();

    return HXHIM_SUCCESS;
}

/**
 * hxhimGetStats
 * Collective operation
 * Each desired pointer should be preallocated with space for md->size values
 *
 * @param hx             the HXHIM session
 * @param dst_rank       the rank that is collecting the data
 * @param put_times      the array of put times from each rank
 * @param num_puts       the array of number of puts from each rank
 * @param get_times      the array of get times from each rank
 * @param num_gets       the array of number of gets from each rank
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimGetStats(hxhim_t *hx, const int dst_rank,
                  uint64_t    *put_times,
                  std::size_t *num_puts,
                  uint64_t    *get_times,
                  std::size_t *num_gets) {
    return hxhim::GetStats(hx, dst_rank,
                           put_times,
                           num_puts,
                           get_times,
                           num_gets);
}

// /**
//  * GetFilled
//  * Collective operation
//  * Collects statistics from all ranks in the communicator
//  *
//  * @param comm        the MPI communicator
//  * @param rank        the rank of this instance of Transport
//  * @param dst_rank    the rank to send to
//  * @param get_bput    whether or not to get bput
//  * @param bput        the array of bput from each rank
//  * @param get_bget    whether or not to get bget
//  * @param bget        the array of bget from each rank
//  * @param get_bgetop  whether or not to get bgetop
//  * @param bgetop      the array of bgetop from each rank
//  * @param get_bdel    whether or not to get bdel
//  * @param bdel        the array of bdel from each rank
//  * @param calc        the function to use to calculate some statistic using the input data
//  * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
//  */
// static int GetFilled(MPI_Comm comm, const int rank, const int dst_rank,
//                      const bool get_bput, long double *bput,
//                      const bool get_bget, long double *bget,
//                      const bool get_bgetop, long double *bgetop,
//                      const bool get_bdel, long double *bdel,
//                      const hxhim_private_t::Stats &stats,
//                      const std::function<long double(const hxhim_private_t::Stats::Op &)> &calc) {
//     MPI_Barrier(comm);

//     if (rank == dst_rank) {
//         if (get_bput) {
//             const long double filled = calc(stats.bput);
//             MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, bput, 1, MPI_LONG_DOUBLE, dst_rank, comm);
//         }
//         if (get_bget) {
//             const long double filled = calc(stats.bget);
//             MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, bget, 1, MPI_LONG_DOUBLE, dst_rank, comm);
//         }
//         if (get_bgetop) {
//             const long double filled = calc(stats.bgetop);
//             MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, bgetop, 1, MPI_LONG_DOUBLE, dst_rank, comm);
//         }
//         if (get_bdel) {
//             const long double filled = calc(stats.bdel);
//             MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, bdel, 1, MPI_LONG_DOUBLE, dst_rank, comm);
//         }
//     }
//     else {
//         if (get_bput) {
//             const long double filled = calc(stats.bput);
//             MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, nullptr, 0, MPI_LONG_DOUBLE, dst_rank, comm);
//         }
//         if (get_bget) {
//             const long double filled = calc(stats.bget);
//             MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, nullptr, 0, MPI_LONG_DOUBLE, dst_rank, comm);
//         }
//         if (get_bgetop) {
//             const long double filled = calc(stats.bgetop);
//             MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, nullptr, 0, MPI_LONG_DOUBLE, dst_rank, comm);
//         }
//         if (get_bdel) {
//             const long double filled = calc(stats.bdel);
//             MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, nullptr, 0, MPI_LONG_DOUBLE, dst_rank, comm);
//         }
//     }

//     MPI_Barrier(comm);

//     return HXHIM_SUCCESS;
// }

// /**
//  * GetMinFilled
//  * Collective operation
//  * Collects statistics from all ranks in the communicator
//  *
//  * @param comm        the MPI communicator
//  * @param dst_rank    the rank to send to
//  * @param get_bput    whether or not to get bput
//  * @param bput        the array of bput from each rank
//  * @param get_bget    whether or not to get bget
//  * @param bget        the array of bget from each rank
//  * @param get_bgetop  whether or not to get bgetop
//  * @param bgetop      the array of bgetop from each rank
//  * @param get_bdel    whether or not to get bdel
//  * @param bdel        the array of bdel from each rank
//  * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
//  */
// int hxhim::GetMinFilled(hxhim_t *hx, const int dst_rank,
//                         const bool get_bput, long double *bput,
//                         const bool get_bget, long double *bget,
//                         const bool get_bgetop, long double *bgetop,
//                         const bool get_bdel, long double *bdel) {
//     auto min_filled = [](const hxhim_private_t::Stats::Op &op) -> long double {
//         std::lock_guard<std::mutex> lock(op.mutex);
//         long double min = op.filled.size()?LDBL_MAX:0;
//         for(REF(op.filled)::value_type const &filled : op.filled) {
//             min = std::min(min, filled.percent);
//         }

//         return min;
//     };

//     return GetFilled(hx->p->bootstrap.comm, hx->p->bootstrap.rank, dst_rank,
//                      get_bput, bput,
//                      get_bget, bget,
//                      get_bgetop, bgetop,
//                      get_bdel, bdel,
//                      hx->p->stats,
//                      min_filled);
// }

// /**
//  * hxhimGetMinFilled
//  * Collective operation
//  * Collects transport statistics from all ranks in the communicator
//  *
//  * @param comm        the MPI communicator
//  * @param rank        the rank of this instance of Transport
//  * @param dst_rank    the rank to send to
//  * @param get_bput    whether or not to get bput
//  * @param bput        the array of bput from each rank
//  * @param get_bget    whether or not to get bget
//  * @param bget        the array of bget from each rank
//  * @param get_bgetop  whether or not to get bgetop
//  * @param bgetop      the array of bgetop from each rank
//  * @param get_bdel    whether or not to get bdel
//  * @param bdel        the array of bdel from each rank
//  * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
//  */
// int hxhimGetMinFilled(hxhim_t *hx, const int dst_rank,
//                                const int get_bput, long double *bput,
//                                const int get_bget, long double *bget,
//                                const int get_bgetop, long double *bgetop,
//                                const int get_bdel, long double *bdel) {
//     return hxhim::GetMinFilled(hx, dst_rank,
//                                get_bput, bput,
//                                get_bget, bget,
//                                get_bgetop, bgetop,
//                                get_bdel, bdel);
// }

// /**
//  * GetAverageFilled
//  * Collective operation
//  * Collects statistics from all ranks in the communicator
//  *
//  * @param comm        the MPI communicator
//  * @param dst_rank    the rank to send to
//  * @param get_bput    whether or not to get bput
//  * @param bput        the array of bput from each rank
//  * @param get_bget    whether or not to get bget
//  * @param bget        the array of bget from each rank
//  * @param get_bgetop  whether or not to get bgetop
//  * @param bgetop      the array of bgetop from each rank
//  * @param get_bdel    whether or not to get bdel
//  * @param bdel        the array of bdel from each rank
//  * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
//  */
// int hxhim::GetAverageFilled(hxhim_t *hx, const int dst_rank,
//                             const bool get_bput, long double *bput,
//                             const bool get_bget, long double *bget,
//                             const bool get_bgetop, long double *bgetop,
//                             const bool get_bdel, long double *bdel) {
//     auto average_filled = [](const hxhim_private_t::Stats::Op &op) -> long double {
//         std::lock_guard<std::mutex> lock(op.mutex);
//         long double sum = 0;
//         for(REF(op.filled)::value_type const &filled : op.filled) {
//             sum += filled.percent;
//         }

//         return op.filled.size()?(sum / op.filled.size()):0;
//     };

//     return GetFilled(hx->p->bootstrap.comm, hx->p->bootstrap.rank, dst_rank,
//                      get_bput, bput,
//                      get_bget, bget,
//                      get_bgetop, bgetop,
//                      get_bdel, bdel,
//                      hx->p->stats,
//                      average_filled);
// }

// /**
//  * hxhimGetAverageFilled
//  * Collective operation
//  * Collects transport statistics from all ranks in the communicator
//  *
//  * @param comm        the MPI communicator
//  * @param rank        the rank of this instance of Transport
//  * @param dst_rank    the rank to send to
//  * @param get_bput    whether or not to get bput
//  * @param bput        the array of bput from each rank
//  * @param get_bget    whether or not to get bget
//  * @param bget        the array of bget from each rank
//  * @param get_bgetop  whether or not to get bgetop
//  * @param bgetop      the array of bgetop from each rank
//  * @param get_bdel    whether or not to get bdel
//  * @param bdel        the array of bdel from each rank
//  * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
//  */
// int hxhimGetAverageFilled(hxhim_t *hx, const int dst_rank,
//                           const int get_bput, long double *bput,
//                           const int get_bget, long double *bget,
//                           const int get_bgetop, long double *bgetop,
//                           const int get_bdel, long double *bdel) {
//     return hxhim::GetAverageFilled(hx, dst_rank,
//                                    get_bput, bput,
//                                    get_bget, bget,
//                                    get_bgetop, bgetop,
//                                    get_bdel, bdel);
// }

// /**
//  * GetMaxFilled
//  * Collective operation
//  * Collects statistics from all ranks in the communicator
//  *
//  * @param comm        the MPI communicator
//  * @param dst_rank    the rank to send to
//  * @param get_bput    whether or not to get bput
//  * @param bput        the array of bput from each rank
//  * @param get_bget    whether or not to get bget
//  * @param bget        the array of bget from each rank
//  * @param get_bgetop  whether or not to get bgetop
//  * @param bgetop      the array of bgetop from each rank
//  * @param get_bdel    whether or not to get bdel
//  * @param bdel        the array of bdel from each rank
//  * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
//  */
// int hxhim::GetMaxFilled(hxhim_t *hx, const int dst_rank,
//                         const bool get_bput, long double *bput,
//                         const bool get_bget, long double *bget,
//                         const bool get_bgetop, long double *bgetop,
//                         const bool get_bdel, long double *bdel) {
//     auto max_filled = [](const hxhim_private_t::Stats::Op &op) -> long double {
//         std::lock_guard<std::mutex> lock(op.mutex);
//         long double max = 0;
//         for(REF(op.filled)::value_type const &filled : op.filled) {
//             max = std::max(max, filled.percent);
//         }

//         return max;
//     };

//     return GetFilled(hx->p->bootstrap.comm, hx->p->bootstrap.rank, dst_rank,
//                      get_bput, bput,
//                      get_bget, bget,
//                      get_bgetop, bgetop,
//                      get_bdel, bdel,
//                      hx->p->stats,
//                      max_filled);
// }

// /**
//  * hxhimGetMaxFilled
//  * Collective operation
//  * Collects transport statistics from all ranks in the communicator
//  *
//  * @param comm        the MPI communicator
//  * @param rank        the rank of this instance of Transport
//  * @param dst_rank    the rank to send to
//  * @param get_bput    whether or not to get bput
//  * @param bput        the array of bput from each rank
//  * @param get_bget    whether or not to get bget
//  * @param bget        the array of bget from each rank
//  * @param get_bgetop  whether or not to get bgetop
//  * @param bgetop      the array of bgetop from each rank
//  * @param get_bdel    whether or not to get bdel
//  * @param bdel        the array of bdel from each rank
//  * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
//  */
// int hxhimGetMaxFilled(hxhim_t *hx, const int dst_rank,
//                       const int get_bput, long double *bput,
//                       const int get_bget, long double *bget,
//                       const int get_bgetop, long double *bgetop,
//                       const int get_bdel, long double *bdel) {
//     return hxhim::GetMaxFilled(hx, dst_rank,
//                                get_bput, bput,
//                                get_bget, bget,
//                                get_bgetop, bgetop,
//                                get_bdel, bdel);
// }
