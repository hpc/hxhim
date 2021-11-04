#include "generic_options.hpp"
#include "hxhim/config.hpp"
#include "hxhim/private/accessors.hpp"

/**
 * This only works when every rank has a range server,
 * which is hy it is not provided as a standard hash.
 */
static int test_hash_local(hxhim_t *hx, void *, const size_t, void *, const size_t, void *) {
    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    return rank;
}

bool fill_options(hxhim_options_t *opts) {
    return ((hxhim_options_init(opts)                                        == HXHIM_SUCCESS) &&
            (hxhim_options_set_mpi_bootstrap(opts, MPI_COMM_WORLD)           == HXHIM_SUCCESS) &&
            (hxhim_options_set_debug_level(opts, MLOG_WARN)                  == HXHIM_SUCCESS) &&
            (hxhim_options_set_client_ratio(opts, 1)                         == HXHIM_SUCCESS) &&
            (hxhim_options_set_server_ratio(opts, 1)                         == HXHIM_SUCCESS) &&
            (hxhim_options_set_datastore_in_memory(opts)                     == HXHIM_SUCCESS) &&
            #ifdef HXHIM_HAVE_THALLIUM
            (hxhim_options_set_transport_thallium(opts, "ofi+tcp")           == HXHIM_SUCCESS) &&
            #else
            (hxhim_options_set_transport_mpi(opts, 1)                        == HXHIM_SUCCESS) &&
            #endif
            (hxhim_options_set_hash_function(opts, "Test_Hash_Local", test_hash_local, nullptr)
                                                                             == HXHIM_SUCCESS) &&
            (hxhim_options_set_maximum_ops_per_request(opts, 1)              == HXHIM_SUCCESS) &&
            (hxhim_options_set_maximum_size_per_request(opts, 1)             == HXHIM_SUCCESS) &&
            (hxhim_options_set_histogram_first_n(opts, 10)                   == HXHIM_SUCCESS) &&
            (hxhim_options_set_histogram_bucket_gen_name(opts, "10_BUCKETS") == HXHIM_SUCCESS) &&
            true);
}
