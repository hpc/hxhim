#include "generic_options.hpp"
#include "hxhim/MaxSize.hpp"
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
            (hxhim_options_set_transport_thallium(opts, "na+sm")                          == HXHIM_SUCCESS) &&
            (hxhim_options_set_hash_name(opts, "MY_RANK")                                 == HXHIM_SUCCESS) &&
            (hxhim_options_set_keys_name(opts, "Keys")                                    == HXHIM_SUCCESS) &&
            (hxhim_options_set_keys_alloc_size(opts, 32)                                  == HXHIM_SUCCESS) &&
            (hxhim_options_set_keys_regions(opts, 1)                                      == HXHIM_SUCCESS) &&
            (hxhim_options_set_buffers_name(opts, "Buffers")                              == HXHIM_SUCCESS) &&
            (hxhim_options_set_buffers_alloc_size(opts, 1)                                == HXHIM_SUCCESS) &&
            (hxhim_options_set_buffers_regions(opts, 1)                                   == HXHIM_SUCCESS) &&
            (hxhim_options_set_ops_cache_name(opts, "OpsCache")                           == HXHIM_SUCCESS) &&
            (hxhim_options_set_ops_cache_alloc_size(opts, hxhim::MaxSize::OpsCache())     == HXHIM_SUCCESS) &&
            (hxhim_options_set_ops_cache_regions(opts, 1)                                 == HXHIM_SUCCESS) &&
            (hxhim_options_set_arrays_name(opts, "Arrays")                                == HXHIM_SUCCESS) &&
            (hxhim_options_set_arrays_alloc_size(opts, 32 * sizeof(void *))               == HXHIM_SUCCESS) &&
            (hxhim_options_set_arrays_regions(opts, 128)                                  == HXHIM_SUCCESS) &&
            (hxhim_options_set_requests_name(opts, "Requests")                            == HXHIM_SUCCESS) &&
            (hxhim_options_set_requests_alloc_size(opts, hxhim::MaxSize::Requests())      == HXHIM_SUCCESS) &&
            (hxhim_options_set_requests_regions(opts, 2)                                  == HXHIM_SUCCESS) &&
            (hxhim_options_set_packed_name(opts, "Packed")                                == HXHIM_SUCCESS) &&
            (hxhim_options_set_packed_alloc_size(opts, 128)                               == HXHIM_SUCCESS) &&
            (hxhim_options_set_packed_regions(opts, 2)                                    == HXHIM_SUCCESS) &&
            (hxhim_options_set_responses_name(opts, "Responses")                          == HXHIM_SUCCESS) &&
            (hxhim_options_set_responses_alloc_size(opts, hxhim::MaxSize::Responses())    == HXHIM_SUCCESS) &&
            (hxhim_options_set_responses_regions(opts, 2)                                 == HXHIM_SUCCESS) &&
            (hxhim_options_set_result_name(opts, "Result")                                == HXHIM_SUCCESS) &&
            (hxhim_options_set_result_alloc_size(opts, hxhim::MaxSize::Result())          == HXHIM_SUCCESS) &&
            (hxhim_options_set_result_regions(opts, 16)                                   == HXHIM_SUCCESS) &&
            (hxhim_options_set_results_name(opts, "Results")                              == HXHIM_SUCCESS) &&
            (hxhim_options_set_results_alloc_size(opts, sizeof(hxhim::Results))           == HXHIM_SUCCESS) &&
            (hxhim_options_set_results_regions(opts, 4)                                   == HXHIM_SUCCESS) &&
            (hxhim_options_set_start_async_put_at(opts, 0)                                == HXHIM_SUCCESS) &&
            (hxhim_options_set_maximum_ops_per_send(opts, 1)                              == HXHIM_SUCCESS) &&
            (hxhim_options_set_histogram_first_n(opts, 10)                                == HXHIM_SUCCESS) &&
            (hxhim_options_set_histogram_bucket_gen_method(opts, "10_BUCKETS")            == HXHIM_SUCCESS) &&
            true);
}
