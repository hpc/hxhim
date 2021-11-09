#include "hxhim/hxhim.hpp"
#include "hxhim/private/Results.hpp"
#include "hxhim/private/accessors.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/private/options.hpp"
#include "hxhim/RangeServer.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

const char *HXHIM_OPEN_ERROR_STR[] = {
    HXHIM_OPEN_ERROR_GEN(HXHIM_OPEN_ERROR_PREFIX, GENERATE_STR)
};

const hxhim_put_permutation_t HXHIM_PUT_PERMUTATIONS[] = {
    HXHIM_PUT_SPO,
    HXHIM_PUT_SOP,
    HXHIM_PUT_PSO,
    HXHIM_PUT_POS,
    HXHIM_PUT_OSP,
    HXHIM_PUT_OPS,
};

const size_t HXHIM_PUT_PERMUTATIONS_COUNT = sizeof(HXHIM_PUT_PERMUTATIONS) / sizeof(hxhim_put_permutation_t);

const char *HXHIM_OP_STR[] = {
    HXHIM_OP_GEN(HXHIM_OP_PREFIX, GENERATE_STR)
};

const char *HXHIM_GETOP_STR[] = {
    HXHIM_GETOP_GEN(HXHIM_GETOP_PREFIX, GENERATE_STR)
};

const char *HXHIM_DATA_STR[] = {
    HXHIM_DATA_GEN(HXHIM_DATA_PREFIX, GENERATE_STR)
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

    int rc = HXHIM_SUCCESS;

    #if PRINT_TIMESTAMPS
    ::Stats::Chronostamp init;
    init.start = ::Stats::now();
    #endif

    //Open mlog - stolen from plfs
    mlog_open((char *) "hxhim", 0, opts->p->debug_level, opts->p->debug_level, nullptr, 0, MLOG_LOGPID, 0);

    mlog(HXHIM_CLIENT_INFO, "Initializing HXHIM");

    hx->p = new hxhim_private_t();

    if (init::bootstrap(hx, opts) != HXHIM_SUCCESS) {
        rc = HXHIM_OPEN_ERROR_BOOTSTRAP;
        goto error;
    }

    if (init::running  (hx, opts) != HXHIM_SUCCESS) {
        rc = HXHIM_OPEN_ERROR_SET_RUNNING;
        goto error;
    }
    if (init::hash     (hx, opts) != HXHIM_SUCCESS) {
        rc = HXHIM_OPEN_ERROR_HASH;
        goto error;
    }

    if (init::datastore(hx, opts) != HXHIM_SUCCESS) {
        rc = HXHIM_OPEN_ERROR_DATASTORE;
        goto error;
    }

    if (init::transport(hx, opts) != HXHIM_SUCCESS) {
        rc = HXHIM_OPEN_ERROR_TRANSPORT;
        goto error;
    }

    if (init::queues   (hx, opts) != HXHIM_SUCCESS) {
        rc = HXHIM_OPEN_ERROR_QUEUES;
        goto error;
    }

    if (init::async_puts(hx, opts) != HXHIM_SUCCESS) {
        rc = HXHIM_OPEN_ERROR_ASYNC_PUT;
        goto error;
    }

    mlog(HXHIM_CLIENT_INFO, "Waiting for everyone to complete initialization");
    MPI_Barrier(hx->p->bootstrap.comm);
    mlog(HXHIM_CLIENT_INFO, "Successfully initialized HXHIM on rank %d/%d", hx->p->bootstrap.rank, hx->p->bootstrap.size);
    #if PRINT_TIMESTAMPS
    init.end = ::Stats::now();
    ::Stats::print_event(hx->p->print_buffer, hx->p->bootstrap.rank, "Open", ::Stats::global_epoch, init);
    #endif
    return rc;

  error:
    if (rc != HXHIM_OPEN_ERROR_BOOTSTRAP) {
        MPI_Barrier(hx->p->bootstrap.comm);
    }
    Close(hx);
    mlog(HXHIM_CLIENT_ERR, "Failed to initialize HXHIM");
    return rc;
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
 * @param db_path  the full path + name of the datastore to open
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
        (init::one_datastore (hx, opts, db_path) != HXHIM_SUCCESS) ||
        (init::queues        (hx, opts)          != HXHIM_SUCCESS) ||
        (init::async_puts    (hx, opts)          != HXHIM_SUCCESS)) {
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
    nocheck::GetMPI(hx, &comm, &rank, &size);

    mlog(HXHIM_CLIENT_INFO, "Rank %d Starting to shutdown HXHIM", rank);

    mlog(HXHIM_CLIENT_DBG, "Rank %d No longer accepting user input", rank);
    destroy::running(hx);

    mlog(HXHIM_CLIENT_DBG, "Rank %d Waiting for all ranks to complete syncing", rank);
    Results::Destroy(hxhim::Sync(hx));
    mlog(HXHIM_CLIENT_DBG, "Rank %d Closing HXHIM", rank);

    // make sure all ranks are ready to stop
    MPI_Barrier(comm);

    destroy::hash(hx);
    destroy::queues(hx);     // clear queued work
    destroy::async_puts(hx); // complete existing processing and prevent new processing
    destroy::transport(hx);  // prevent work from getting into transport
    destroy::datastore(hx);

    // stats should not be modified any more

    // print stats here for now
    std::stringstream s;
    print_stats(hx, s);
    mlog(HXHIM_CLIENT_NOTE, "Rank %d Stats\n%s", rank, s.str().c_str());

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

    if (hx->p->range_server.datastore) {
        // change datastores
        std::stringstream s;
        s << name << "-" << RangeServer::get_id(hx->p->bootstrap.rank,
                                                hx->p->bootstrap.size,
                                                hx->p->range_server.client_ratio,
                                                hx->p->range_server.server_ratio);
        hx->p->range_server.datastore->Open(s.str());
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
 * ChangeHash
 * Close the current datastore and open a new one with the same hash function.
 * This is a collective function, but allows for each rank to select its own
 * new datastore name.
 *
 * @param hx                 the HXHIM session
 * @param name               the name of the new datastore
 * @param write_histograms   whether or not to write the old datastore's histograms
 * @param read_histograms    whether or not to try to find and read the new datastore's histograms
 * @param create_missing     whether or not to create histograms that were not found
 * @return A list of results from before the datastores were changed
 */
hxhim::Results *hxhim::ChangeDatastore(hxhim_t *hx, const char *name,
                                       const bool write_histograms, const bool read_histograms,
                                       const bool create_missing) {
    if (!hxhim::valid(hx)) {
        return nullptr;
    }

    hxhim::Results *res = hxhim::Sync(hx);

    MPI_Barrier(hx->p->bootstrap.comm);

    hx->p->range_server.datastore->Change(std::string(name),
                                          write_histograms,
                                          read_histograms?&hx->p->histograms.names:nullptr);

    if (create_missing) {
        const Datastore::Datastore::Histograms *hists = nullptr;
        hx->p->range_server.datastore->GetHistograms(&hists);

        for(std::string const &hist_name : hx->p->histograms.names) {
            if (hists->find(hist_name) == hists->end()){
                hx->p->range_server.datastore->AddHistogram(hist_name, hx->p->histograms.config);
            }
        }
    }

    MPI_Barrier(hx->p->bootstrap.comm);

    return res;
}

/**
 * ChangeHash
 * Close the current datastore and open a new one with the same hash function.
 * This is a collective function, but allows for each rank to select its own
 * new datastore name.
 *
 * @param hx                 the HXHIM session
 * @param name               the name of the new datastore
 * @param write_histograms   whether or not to write the old datastore's histograms
 * @param read_histograms    whether or not to try to find and read the new datastore's histograms
 * @param create_missing     whether or not to create histograms that were not found
 * @return A list of results from before the datastores were changed
 */
hxhim_results_t *hxhimChangeDatastore(hxhim_t *hx, const char *name,
                                      const int write_histograms, const int read_histograms,
                                      const int create_missing) {
    return hxhim_results_init(hx, hxhim::ChangeDatastore(hx, name, write_histograms, read_histograms, create_missing));
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
    uint64_t    local_put_times = 0;
    std::size_t local_num_puts  = 0;
    uint64_t    local_get_times = 0;
    std::size_t local_num_gets  = 0;

    if (hx->p->range_server.datastore->GetStats(&local_put_times,
                                                &local_num_puts,
                                                &local_get_times,
                                                &local_num_gets) != DATASTORE_SUCCESS) {
        return HXHIM_ERROR;
    }

    // send to destination rank
    MPI_Comm comm = hx->p->bootstrap.comm;
    MPI_Barrier(comm);

    if (put_times) {
        if (MPI_Gather(&local_put_times, 1, MPI_UINT64_T,
                              put_times, 1, MPI_UINT64_T, dst_rank, comm) != MPI_SUCCESS) {
            return HXHIM_ERROR;
        }
    }

    if (num_puts) {
        if (MPI_Gather(&local_num_puts, size_size, MPI_CHAR,
                              num_puts, size_size, MPI_CHAR, dst_rank, comm) != MPI_SUCCESS) {
            return HXHIM_ERROR;
        }
    }

    if (get_times) {
        if (MPI_Gather(&local_get_times, 1, MPI_UINT64_T,
                              get_times, 1, MPI_UINT64_T, dst_rank, comm) != MPI_SUCCESS) {
            return HXHIM_ERROR;
        }
    }

    if (num_gets) {
        if (MPI_Gather(&local_num_gets, size_size, MPI_CHAR,
                              num_gets, size_size, MPI_CHAR, dst_rank, comm) != MPI_SUCCESS) {
            return HXHIM_ERROR;
        }
    }

    MPI_Barrier(comm);

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
