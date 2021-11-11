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

bool fill_options(hxhim_t *hx) {
    return ((hxhim_set_debug_level(hx, MLOG_WARN)                  == HXHIM_SUCCESS) &&
            (hxhim_set_client_ratio(hx, 1)                         == HXHIM_SUCCESS) &&
            (hxhim_set_server_ratio(hx, 1)                         == HXHIM_SUCCESS) &&
            (hxhim_set_datastores_per_server(hx, 1)                == HXHIM_SUCCESS) &&
            (hxhim_set_datastore_in_memory(hx)                     == HXHIM_SUCCESS) &&
            #ifdef HXHIM_HAVE_THALLIUM
            (hxhim_set_transport_thallium(hx, "ofi+tcp")           == HXHIM_SUCCESS) &&
            #else
            (hxhim_set_transport_mpi(hx, 1)                        == HXHIM_SUCCESS) &&
            #endif
            (hxhim_set_hash_function(hx, "Test_Hash_Local", test_hash_local, nullptr)
                                                                   == HXHIM_SUCCESS) &&
            (hxhim_set_start_async_puts_at(hx, 0)                  == HXHIM_SUCCESS) &&
            (hxhim_set_maximum_ops_per_request(hx, 1)              == HXHIM_SUCCESS) &&
            (hxhim_set_maximum_size_per_request(hx, 1)             == HXHIM_SUCCESS) &&
            (hxhim_set_histogram_first_n(hx, 10)                   == HXHIM_SUCCESS) &&
            (hxhim_set_histogram_bucket_gen_name(hx, "10_BUCKETS") == HXHIM_SUCCESS) &&
            true);
}
