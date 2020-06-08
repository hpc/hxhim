#include "generic_options.hpp"
#include "hxhim/Results.hpp"
#include "hxhim/config.hpp"

bool fill_options(hxhim_options_t *opts) {
    return ((hxhim_options_init(opts)                                                     == HXHIM_SUCCESS) &&
            (hxhim_options_set_mpi_bootstrap(opts, MPI_COMM_WORLD)                        == HXHIM_SUCCESS) &&
            (hxhim_options_set_debug_level(opts, MLOG_ERR)                                == HXHIM_SUCCESS) &&
            (hxhim_options_set_client_ratio(opts, 1)                                      == HXHIM_SUCCESS) &&
            (hxhim_options_set_server_ratio(opts, 1)                                      == HXHIM_SUCCESS) &&
            (hxhim_options_set_datastores_per_range_server(opts, 1)                       == HXHIM_SUCCESS) &&
            (hxhim_options_set_datastore_in_memory(opts)                                  == HXHIM_SUCCESS) &&
            #ifdef HXHIM_HAVE_THALLIUM
            (hxhim_options_set_transport_thallium(opts, "na+sm", 1024)                    == HXHIM_SUCCESS) &&
            #else
            (hxhim_options_set_transport_mpi(opts, 1)                                     == HXHIM_SUCCESS) &&
            #endif
            (hxhim_options_set_hash_name(opts, "MY_RANK")                                 == HXHIM_SUCCESS) &&
            (hxhim_options_set_start_async_put_at(opts, 0)                                == HXHIM_SUCCESS) &&
            (hxhim_options_set_maximum_ops_per_send(opts, 1)                              == HXHIM_SUCCESS) &&
            (hxhim_options_set_histogram_first_n(opts, 10)                                == HXHIM_SUCCESS) &&
            (hxhim_options_set_histogram_bucket_gen_method(opts, "10_BUCKETS")            == HXHIM_SUCCESS) &&
            true);
}
