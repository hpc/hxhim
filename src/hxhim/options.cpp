#include <cmath>

#include "datastore/datastores.hpp"
#include "hxhim/config.hpp"
#include "hxhim/options.hpp"
#include "hxhim/private/hxhim.hpp"
#include "transport/backend/MPI/Options.hpp"
#if HXHIM_HAVE_THALLIUM
#include "transport/backend/Thallium/Options.hpp"
#endif
#include "utils/memory.hpp"

/**
 * hxhim_set_mpi_bootstrap
 *
 * @param hx    the set of options to be modified
 * @param comm  the MPI communicator HXHIM will use to bootstrap
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_set_mpi_bootstrap(hxhim_t *hx, MPI_Comm comm) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    return ((hx->p->bootstrap.comm = comm) == MPI_COMM_NULL)?HXHIM_ERROR:HXHIM_SUCCESS;
}

/**
 * hxhim_set_debug_level
 *
 * @param hx     the set of options to be modified
 * @param level  the mlog level
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_set_debug_level(hxhim_t *hx, const int level) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    hx->p->debug_level = level;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_set_client_ratio
 *
 * @param hx     the set of options to be modified
 * @param count  the ratio of clients
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_set_client_ratio(hxhim_t *hx, const size_t ratio) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    if (ratio < 1) {
        return HXHIM_ERROR;
    }

    hx->p->range_server.client_ratio = ratio;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_set_server_ratio
 *
 * @param hx     the set of options to be modified
 * @param ratio  the ratio of servers
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_set_server_ratio(hxhim_t *hx, const size_t ratio) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    if (ratio < 1) {
        return HXHIM_ERROR;
    }

    hx->p->range_server.server_ratio = ratio;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_set_datastores_per_server
 * Sets how many datastores to open per range server
 *
 * @param hx                      the set of options to be modified
 * @param datastores_per_server   the number of datastores per server to open
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_set_datastores_per_server(hxhim_t *hx, const size_t datastores_per_server) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    if (!datastores_per_server) {
        return HXHIM_ERROR;
    }

    hx->p->range_server.datastores.per_server = datastores_per_server;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_set_open_init_datastore
 * Sets whether or not initialization should open datastores after setting up their wrappers.
 *
 * @param hx    the set of options to be modified
 * @param init  whether or not the underlying datastore should be opened when initializing
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_set_open_init_datastore(hxhim_t *hx, const int init) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    hx->p->range_server.datastores.open_init = !!init;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_datastore_histograms
 * Set whether or not to read histograms when opening datastores
 * and whether or not to write histograms when closing datastores.
 *
 * @param read_histograms whether or not to try to read existing histograms from the datastore when opening
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_datastore_histograms(hxhim_t *hx, const int read, const int write) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    hx->p->histograms.read = read;
    hx->p->histograms.write = write;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_set_datastore
 * Sets the values needed to set up the datastore
 * This function moves ownership of the config function from the caller to hx
 *
 * @param hx       the set of options to be modified
 * @param config   configuration data needed to initialize the datastore
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
static int hxhim_set_datastore(hxhim_t *hx, Datastore::Config *config) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    destruct(hx->p->range_server.datastores.config);

    hx->p->range_server.datastores.config = config;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_create_in_memory_config
 *
 * @return a pointer to the configuration data, or a nullptr
 */
static Datastore::Config *hxhim_create_in_memory_config() {
    return construct<Datastore::InMemory::Config>();
}

int hxhim_set_datastore_name(hxhim_t *hx, const char *prefix, const char *basename, const char *postfix) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    hx->p->range_server.datastores.prefix = prefix;
    hx->p->range_server.datastores.basename = basename;
    hx->p->range_server.datastores.postfix = postfix;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_set_datastore_in_memory
 * Sets up the values needed for a in_memory datastore
 *
 * @param hx    the set of options to be modified
 * @param path  the name prefix for each datastore
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_set_datastore_in_memory(hxhim_t *hx) {
    Datastore::Config *config = hxhim_create_in_memory_config();
    if (hxhim_set_datastore(hx, config) != HXHIM_SUCCESS) {
        destruct(config);
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

#if HXHIM_HAVE_LEVELDB
/**
 * hxhim_set_datastore_leveldb
 * Sets up the values needed for a leveldb datastore
 *
 * @param hx                  the set of options to be modified
 * @param create_if_missing   whether or not to create the datastore if it does not already exist
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_set_datastore_leveldb(hxhim_t *hx, const int create_if_missing) {
    Datastore::LevelDB::Config *config = construct<Datastore::LevelDB::Config>(create_if_missing);
    if (hxhim_set_datastore(hx, config) != HXHIM_SUCCESS) {
        destruct(config);
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}
#endif

#if HXHIM_HAVE_ROCKSDB
/**
 * hxhim_set_datastore_rocksdb
 * Sets up the values needed for a rocksdb datastore
 *
 * @param hx                  the set of options to be modified
 * @param prefix              the path prefix for each datastore
 * @param postfix             any extra string to add to the back of the datastore name
 * @param create_if_missing   whether or not to create the datastore if it does not already exist
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_set_datastore_rocksdb(hxhim_t *hx, const int create_if_missing) {
    Datastore::RocksDB::Config *config = construct<Datastore::RocksDB::Config>(create_if_missing);
    if (hxhim_set_datastore(hx, config) != HXHIM_SUCCESS) {
        destruct(config);
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}
#endif

/**
 * hxhim_set_hash_name
 * Sets the hash function to use to determine where a key should go
 * This function takes the hash name instead of a function because
 * the extra arguments cannot always be determined before HXHIM starts (?).
 *
 * @param hx     the set of options to be modified
 * @param hash   the name of the hash function
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_set_hash_name(hxhim_t *hx, const char *name) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    const std::string hash_name = name;
    std::unordered_map<std::string, hxhim_hash_t>::const_iterator it = hxhim::config::HASHES.find(hash_name);
    if (it == hxhim::config::HASHES.end()) {
        return HXHIM_ERROR;
    }

    hx->p->hash.name = std::move(hash_name);
    hx->p->hash.func = it->second;
    hx->p->hash.args = nullptr;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_set_hash_function
 * Sets the hash function to use to determine where a key should go
 * This function takes the hash name instead of a function because
 * the extra arguments cannot always be determined before HXHIM starts (?).
 *
 * @param hx     the set of options to be modified
 * @param hash   the function to use as the hash function
 * @param args   the exta arguments to pass into the hash function
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_set_hash_function(hxhim_t *hx, const char *name, hxhim_hash_t func, void *args) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    if (!func) {
        return HXHIM_ERROR;
    }

    hx->p->hash.name = name;
    hx->p->hash.func = func;
    hx->p->hash.args = args;

    return HXHIM_SUCCESS;
}

int hxhim_set_transform_numeric_values(hxhim_t *hx, const char neg, const char pos, const size_t float_precision, const size_t double_precision) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    hx->p->range_server.datastores.transform.numeric_extra.neg = neg;
    hx->p->range_server.datastores.transform.numeric_extra.pos = pos;
    hx->p->range_server.datastores.transform.numeric_extra.float_precision = float_precision;
    hx->p->range_server.datastores.transform.numeric_extra.double_precision = double_precision;

    return HXHIM_SUCCESS;
}

int hxhim_set_transform_function(hxhim_t *hx, const enum hxhim_data_t type,
                                         hxhim_encode_func encode, void *encode_extra,
                                         hxhim_decode_func decode, void *decode_extra) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    hx->p->range_server.datastores.transform.encode.emplace(type, std::make_pair(encode, encode_extra));
    hx->p->range_server.datastores.transform.decode.emplace(type, std::make_pair(decode, decode_extra));

    return HXHIM_SUCCESS;
}

/**
 * hxhim_set_transport
 * Sets the values needed to set up the Transport
 * This function moves ownership of the config function from the caller to hx
 *
 * @param hx       the set of options to be modified
 * @param config   configuration data needed to initialize the Transport
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
static int hxhim_set_transport(hxhim_t *hx, Transport::Options *config) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    destruct(hx->p->transport.config);

    hx->p->transport.config = config;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_set_transport_null
 * Sets the values needed to set up a null Transport
 *
 * @param hx  the set of options to be modified
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_set_transport_null(hxhim_t *hx) {
    Transport::Options *config = construct<Transport::Options>(Transport::TRANSPORT_NULL);
    if (hxhim_set_transport(hx, config) != HXHIM_SUCCESS) {
        destruct(config);
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim_set_transport_mpi
 * Sets the values needed to set up a mpi Transport
 *
 * @param hx           the set of options to be modified
 * @param listeners    the number of listeners
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_set_transport_mpi(hxhim_t *hx, const size_t listeners) {
    Transport::Options *config = construct<Transport::MPI::Options>(hx->p->bootstrap.comm, listeners);
    if (hxhim_set_transport(hx, config) != HXHIM_SUCCESS) {
        destruct(config);
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

#if HXHIM_HAVE_THALLIUM
/**
 * hxhim_set_transport_thallium
 * Sets the values needed to set up a thallium Transport
 *
 * @param hx       the set of options to be modified
 * @param module   the name of the thallium module to use
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_set_transport_thallium(hxhim_t *hx, const std::string &module) {
    Transport::Options *config = construct<Transport::Thallium::Options>(module);
    if (hxhim_set_transport(hx, config) != HXHIM_SUCCESS) {
        destruct(config);
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim_set_transport_thallium
 * Sets the values needed to set up a thallium Transport
 *
 * @param hx       the set of options to be modified
 * @param module   the name of the thallium module to use
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_set_transport_thallium(hxhim_t *hx, const char *module) {
    return hxhim_set_transport_thallium(hx, std::string(module));
}
#endif

/**
 * hxhim_add_endpoint_to_group
 * Adds an endpoint to the endpoint group
 *
 * @param hx   the set of options to be modified
 * @param id   the unique id to add
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_add_endpoint_to_group(hxhim_t *hx, const int id) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    if (id < 0) {
        return HXHIM_ERROR;
    }

    hx->p->transport.endpointgroup.insert(id);

    return HXHIM_SUCCESS;
}

/**
 * hxhim_clear_endpoint_group
 * Removes all endpoints in the endpoint group
 *
 * @param hx   the set of options to be modified
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_clear_endpoint_group(hxhim_t *hx) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    hx->p->transport.endpointgroup.clear();

    return HXHIM_SUCCESS;
}

/**
 * hxhim_set_start_async_puts_at
 * Set the number of bulk PUTs to queue up before flushing in the background thread
 *
 * @param hx     the set of options to be modified
 * @param count  the number of PUTs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_set_start_async_puts_at(hxhim_t *hx, const std::size_t count) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    hx->p->async_puts.enabled = !!count;
    hx->p->async_puts.max_queued = count;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_set_maximum_ops_per_request
 * Set the maximum number of operations a bulk request can hold
 *
 * @param hx     the set of options to be modified
 * @param count  the number of PUTs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_set_maximum_ops_per_request(hxhim_t *hx, const std::size_t count) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    hx->p->queues.max_per_request.ops = count;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_set_maximum_size_per_request
 * Set the maximum number of bytes a bulk requst can be
 *
 * @param hx     the set of options to be modified
 * @param count  the number of PUTs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_set_maximum_size_per_request(hxhim_t *hx, const std::size_t size) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    hx->p->queues.max_per_request.size = size;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_set_histogram_first_n
 * Set the number of datapoints to use to generate the histogram buckets
 *
 * @param hx     the set of options to be modified
 * @param count  the number of datapoints
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_set_histogram_first_n(hxhim_t *hx, const std::size_t count) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    hx->p->histograms.config.first_n = count;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_set_histogram_bucket_gen_name
 * Set the name of the bucket generation method.
 * This function only allows for predefined functions
 * because the function signature uses C++ objects
 *
 * @param hx      the set of options to be modified
 * @param method  the name of the method
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_set_histogram_bucket_gen_name(hxhim_t *hx, const char *method) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    decltype(hxhim::config::HISTOGRAM_BUCKET_GENERATORS)::const_iterator gen_it = hxhim::config::HISTOGRAM_BUCKET_GENERATORS.find(method);
    if (gen_it == hxhim::config::HISTOGRAM_BUCKET_GENERATORS.end()) {
        return HXHIM_ERROR;
    }

    decltype(hxhim::config::HISTOGRAM_BUCKET_GENERATOR_EXTRA_ARGS)::const_iterator args_it = hxhim::config::HISTOGRAM_BUCKET_GENERATOR_EXTRA_ARGS.find(method);
    if (args_it == hxhim::config::HISTOGRAM_BUCKET_GENERATOR_EXTRA_ARGS.end()) {
        return HXHIM_ERROR;
    }

    hx->p->histograms.config.generator = gen_it->second;
    hx->p->histograms.config.extra_args = args_it->second;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_set_histogram_bucket_gen_name
 * Set the name of the bucket generation method.
 * This function only allows for predefined functions
 * because the function signature uses C++ objects
 *
 * @param hx      the set of options to be modified
 * @param method  the name of the method
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_set_histogram_bucket_gen_name(hxhim_t *hx, const std::string &method) {
    return hxhim_set_histogram_bucket_gen_name(hx, method.c_str());
}

/**
 * hxhim_set_histogram_bucket_gen_function
 * Set the bucket generation function.
 * This function allows for arbitrary functions
 * that match the histogram bucket generator function
 * signature to be used.
 *
 * @param hx    the set of options to be modified
 * @param gen   the custom function for generating buckets
 * @param args  arguments the custom function will use at runtime
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_set_histogram_bucket_gen_function(hxhim_t *hx, HistogramBucketGenerator_t gen, void *args) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    if (!gen) {
        return HXHIM_ERROR;
    }

    hx->p->histograms.config.generator = gen;
    hx->p->histograms.config.extra_args = args;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_add_histogram_track_predicate
 *
 * @param hx         the set of options to be modified
 * @param name       the name of the predicate to track
 * @param name_len   length of name argument
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_add_histogram_track_predicate(hxhim_t *hx, const char *name, const size_t name_len) {
    return hxhim_add_histogram_track_predicate(hx, std::string(name, name_len));
}

/**
 * hxhim_add_histogram_track_predicate
 *
 * @param hx      the set of options to be modified
 * @param name    the name of the predicate to track
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_add_histogram_track_predicate(hxhim_t *hx, const std::string &name) {
    if (!hx || !hx->p || hx->p->running) {
        return HXHIM_ERROR;
    }

    hx->p->histograms.names.insert(name);
    return HXHIM_SUCCESS;
}
