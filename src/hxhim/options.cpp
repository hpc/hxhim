#include <cmath>

#include "hxhim/config.hpp"
#include "hxhim/private/options.hpp"
#include "transport/backend/MPI/Options.hpp"
#if HXHIM_HAVE_THALLIUM
#include "transport/backend/Thallium/Options.hpp"
#endif
#include "utils/memory.hpp"

/**
 * valid
 * Checks if opts are valid
 *
 * @param opts the HXHIM options
 * @param true if ready, else false
 */
bool hxhim::valid(hxhim_options_t *opts) {
    return opts && opts->p;
}

/**
 * hxhim_options_init
 * Allocates the private pointer of an existing hxhim_options_t structure
 *
 * @param opts address of an existing hxhim_options_t structure
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_init(hxhim_options_t *opts) {
    ::Stats::init();

    if (!opts) {
        return HXHIM_ERROR;
    }

    opts->p = construct<hxhim_options_private_t>();

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_mpi_bootstrap
 *
 * @param opts the set of options to be modified
 * @param comm the MPI communicator HXHIM will use to bootstrap
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_mpi_bootstrap(hxhim_options_t *opts, MPI_Comm comm) {
    if (!hxhim::valid(opts)) {
        return HXHIM_ERROR;
    }

    return ((opts->p->comm = comm) == MPI_COMM_NULL)?HXHIM_ERROR:HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_debug_level
 *
 * @param opts   the set of options to be modified
 * @param level  the mlog level
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_debug_level(hxhim_options_t *opts, const int level) {
    if (!hxhim::valid(opts)) {
        return HXHIM_ERROR;
    }

    opts->p->debug_level = level;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_client_ratio
 *
 * @param opts   the set of options to be modified
 * @param count  the ratio of clients
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_client_ratio(hxhim_options_t *opts, const size_t ratio) {
    if (!hxhim::valid(opts)) {
        return HXHIM_ERROR;
    }

    if (ratio < 1) {
        return HXHIM_ERROR;
    }

    opts->p->client_ratio = ratio;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_server_ratio
 *
 * @param opts   the set of options to be modified
 * @param ratio  the ratio of servers
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_server_ratio(hxhim_options_t *opts, const size_t ratio) {
    if (!hxhim::valid(opts)) {
        return HXHIM_ERROR;
    }

    if (ratio < 1) {
        return HXHIM_ERROR;
    }

    opts->p->server_ratio = ratio;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_open_init_datastore
 * Sets whether or not initialization should open datastores after setting up their wrappers.
 *
 * @param opts             the set of options to be modified
 * @param init             whether or not the underlying datastore should be opened when initializing
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_open_init_datastore(hxhim_options_t *opts, const int init) {
    if (!hxhim::valid(opts)) {
        return HXHIM_ERROR;
    }

    opts->p->open_init_datastore = init;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_datastore_histograms
 * Set whether or not to read histograms when opening datastores
 * and whether or not to write histograms when closing datastores.
 *
 * @param read_histograms  whether or not to try to read existing histograms from the datastore when opening
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_datastore_histograms(hxhim_options_t *opts, const int read, const int write) {
    if (!hxhim::valid(opts)) {
        return HXHIM_ERROR;
    }

    opts->p->histograms.read = read;
    opts->p->histograms.write = write;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_datastore
 * Sets the values needed to set up the datastore
 * This function moves ownership of the config function from the caller to opts
 *
 * @param opts   the set of options to be modified
 * @param config configuration data needed to initialize the datastore
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
static int hxhim_options_set_datastore(hxhim_options_t *opts, datastore::Config *config) {
    if (!hxhim::valid(opts)) {
        return HXHIM_ERROR;
    }

    destruct(opts->p->datastore);

    opts->p->datastore = config;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_create_in_memory_config
 *
 * @return a pointer to the configuration data, or a nullptr
 */
static datastore::Config *hxhim_options_create_in_memory_config() {
    return construct<datastore::InMemory::Config>();
}

/**
 * hxhim_options_set_datastore_in_memory
 * Sets up the values needed for a in_memory datastore
 *
 * @param opts   the set of options to be modified
 * @param path the name prefix for each datastore
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_datastore_in_memory(hxhim_options_t *opts) {
    datastore::Config *config = hxhim_options_create_in_memory_config();
    if (hxhim_options_set_datastore(opts, config) != HXHIM_SUCCESS) {
        destruct(config);
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

#if HXHIM_HAVE_LEVELDB
/**
 * hxhim_options_set_datastore_leveldb
 * Sets up the values needed for a leveldb datastore
 *
 * @param opts                the set of options to be modified
 * @param prefix              the path prefix for each datastore
 * @param postfix             any extra string to add to the back of the datastore name
 * @param create_if_missing   whether or not to create the datastore if it does not already exist
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_datastore_leveldb(hxhim_options_t *opts, const char *prefix, const char *postfix, const int create_if_missing) {
    datastore::leveldb::Config *config = construct<datastore::leveldb::Config>(prefix, postfix, create_if_missing);
    if (hxhim_options_set_datastore(opts, config) != HXHIM_SUCCESS) {
        destruct(config);
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}
#endif

#if HXHIM_HAVE_ROCKSDB
/**
 * hxhim_options_set_datastore_rocksdb
 * Sets up the values needed for a rocksdb datastore
 *
 * @param opts                the set of options to be modified
 * @param prefix              the path prefix for each datastore
 * @param postfix             any extra string to add to the back of the datastore name
 * @param create_if_missing   whether or not to create the datastore if it does not already exist
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_datastore_rocksdb(hxhim_options_t *opts, const char *prefix, const char *postfix, const int create_if_missing) {
    datastore::rocksdb::Config *config = construct<datastore::rocksdb::Config>(prefix, postfix, create_if_missing);
    if (hxhim_options_set_datastore(opts, config) != HXHIM_SUCCESS) {
        destruct(config);
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}
#endif

/**
 * hxhim_options_set_hash_name
 * Sets the hash function to use to determine where a key should go
 * This function takes the hash name instead of a function because
 * the extra arguments cannot always be determined before HXHIM starts (?).
 *
 * @param opts   the set of options to be modified
 * @param hash   the name of the hash function
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_hash_name(hxhim_options_t *opts, const char *name) {
    if (!hxhim::valid(opts)) {
        return HXHIM_ERROR;
    }

    const std::string hash_name = name;
    std::unordered_map<std::string, hxhim_hash_t>::const_iterator it = hxhim::config::HASHES.find(hash_name);
    if (it == hxhim::config::HASHES.end()) {
        return HXHIM_ERROR;
    }

    opts->p->hash.name = std::move(hash_name);
    opts->p->hash.func = it->second;
    opts->p->hash.args = nullptr;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_hash_function
 * Sets the hash function to use to determine where a key should go
 * This function takes the hash name instead of a function because
 * the extra arguments cannot always be determined before HXHIM starts (?).
 *
 * @param opts   the set of options to be modified
 * @param hash   the function to use as the hash function
 * @param args   the exta arguments to pass into the hash function
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_hash_function(hxhim_options_t *opts, const char *name, hxhim_hash_t func, void *args) {
    if (!hxhim::valid(opts)) {
        return HXHIM_ERROR;
    }

    if (!func) {
        return HXHIM_ERROR;
    }

    opts->p->hash.name = name;
    opts->p->hash.func = func;
    opts->p->hash.args = args;

    return HXHIM_SUCCESS;
}

int hxhim_options_set_transform_numeric_values(hxhim_options_t *opts, const char neg, const char pos, const size_t float_precision, const size_t double_precision) {
    if (!hxhim::valid(opts)) {
        return HXHIM_ERROR;
    }

    opts->p->transform.numeric_extra.neg = neg;
    opts->p->transform.numeric_extra.pos = pos;
    opts->p->transform.numeric_extra.float_precision = float_precision;
    opts->p->transform.numeric_extra.double_precision = double_precision;

    return HXHIM_SUCCESS;
}

int hxhim_options_set_transform_function(hxhim_options_t *opts, const enum hxhim_data_t type,
                                         hxhim_encode_func encode, void *encode_extra,
                                         hxhim_decode_func decode, void *decode_extra) {
    if (!hxhim::valid(opts)) {
        return HXHIM_ERROR;
    }

    opts->p->transform.encode.emplace(type, std::make_pair(encode, encode_extra));
    opts->p->transform.decode.emplace(type, std::make_pair(decode, decode_extra));

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_transport
 * Sets the values needed to set up the Transport
 * This function moves ownership of the config function from the caller to opts
 *
 * @param opts   the set of options to be modified
 * @param config configuration data needed to initialize the Transport
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
static int hxhim_options_set_transport(hxhim_options_t *opts, Transport::Options *config) {
    if (!hxhim::valid(opts)) {
        return HXHIM_ERROR;
    }

    destruct(opts->p->transport);

    opts->p->transport = config;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_transport_null
 * Sets the values needed to set up a null Transport
 *
 * @param opts              the set of options to be modified
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_transport_null(hxhim_options_t *opts) {
    Transport::Options *config = construct<Transport::Options>(Transport::TRANSPORT_NULL);
    if (hxhim_options_set_transport(opts, config) != HXHIM_SUCCESS) {
        destruct(config);
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_transport_mpi
 * Sets the values needed to set up a mpi Transport
 *
 * @param opts              the set of options to be modified
 * @param listeners         the number of listeners
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_transport_mpi(hxhim_options_t *opts, const size_t listeners) {
    Transport::Options *config = construct<Transport::MPI::Options>(opts->p->comm, listeners);
    if (hxhim_options_set_transport(opts, config) != HXHIM_SUCCESS) {
        destruct(config);
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

#if HXHIM_HAVE_THALLIUM
/**
 * hxhim_options_set_transport_thallium
 * Sets the values needed to set up a thallium Transport
 *
 * @param opts     the set of options to be modified
 * @param module   the name of the thallium module to use
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_transport_thallium(hxhim_options_t *opts, const std::string &module) {
    Transport::Options *config = construct<Transport::Thallium::Options>(module);
    if (hxhim_options_set_transport(opts, config) != HXHIM_SUCCESS) {
        destruct(config);
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_transport_thallium
 * Sets the values needed to set up a thallium Transport
 *
 * @param opts     the set of options to be modified
 * @param module   the name of the thallium module to use
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_transport_thallium(hxhim_options_t *opts, const char *module) {
    return hxhim_options_set_transport_thallium(opts, std::string(module));
}
#endif

/**
 * hxhim_options_add_endpoint_to_group
 * Adds an endpoint to the endpoint group
 *
 * @param opts   the set of options to be modified
 * @param id     the unique id to add
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_add_endpoint_to_group(hxhim_options_t *opts, const int id) {
    if (!hxhim::valid(opts)) {
        return HXHIM_ERROR;
    }

    if (id < 0) {
        return HXHIM_ERROR;
    }

    opts->p->endpointgroup.insert(id);

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_clear_endpoint_group
 * Removes all endpoints in the endpoint group
 *
 * @param opts   the set of options to be modified
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_clear_endpoint_group(hxhim_options_t *opts) {
    if (!hxhim::valid(opts)) {
        return HXHIM_ERROR;
    }

    opts->p->endpointgroup.clear();

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_start_async_put_at
 * Set the number of bulk PUTs to queue up before flushing in the background thread
 *
 * @param opts  the set of options to be modified
 * @param count the number of PUTs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_start_async_put_at(hxhim_options_t *opts, const std::size_t count) {
    if (!hxhim::valid(opts)) {
        return HXHIM_ERROR;
    }

    opts->p->start_async_put_at = count;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_maximum_ops_per_send
 * Set the number of operations to queue up before flushing in the background thread
 *
 * @param opts  the set of options to be modified
 * @param count the number of PUTs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_maximum_ops_per_send(hxhim_options_t *opts, const std::size_t count) {
    if (!hxhim::valid(opts)) {
        return HXHIM_ERROR;
    }

    opts->p->max_ops_per_send = count;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_histogram_first_n
 * Set the number of datapoints to use to generate the histogram buckets
 *
 * @param opts  the set of options to be modified
 * @param count the number of datapoints
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_histogram_first_n(hxhim_options_t *opts, const std::size_t count) {
    if (!hxhim::valid(opts)) {
        return HXHIM_ERROR;
    }

    opts->p->histograms.config.first_n = count;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_histogram_bucket_gen_name
 * Set the name of the bucket generation method.
 * This function only allows for predefined functions
 * because the function signature uses C++ objects
 *
 * @param opts   the set of options to be modified
 * @param method the name of the method
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_histogram_bucket_gen_name(hxhim_options_t *opts, const char *method) {
    if (!hxhim::valid(opts)) {
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

    opts->p->histograms.config.generator = gen_it->second;
    opts->p->histograms.config.extra_args = args_it->second;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_histogram_bucket_gen_name
 * Set the name of the bucket generation method.
 * This function only allows for predefined functions
 * because the function signature uses C++ objects
 *
 * @param opts   the set of options to be modified
 * @param method the name of the method
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_histogram_bucket_gen_name(hxhim_options_t *opts, const std::string &method) {
    return hxhim_options_set_histogram_bucket_gen_name(opts, method.c_str());
}

/**
 * hxhim_options_set_histogram_bucket_gen_function
 * Set the bucket generation function.
 * This function allows for arbitrary functions
 * that match the histogram bucket generator function
 * signature to be used.
 *
 * @param opts   the set of options to be modified
 * @param gen    the custom function for generating buckets
 * @param args   arguments the custom function will use at runtime
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_histogram_bucket_gen_function(hxhim_options_t *opts, HistogramBucketGenerator_t gen, void *args) {
    if (!hxhim::valid(opts)) {
        return HXHIM_ERROR;
    }

    if (!gen) {
        return HXHIM_ERROR;
    }

    opts->p->histograms.config.generator = gen;
    opts->p->histograms.config.extra_args = args;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_add_histogram_track_predicate
 *
 * @param opts       the set of options to be modified
 * @param name       the name of the predicate to track
 * @param name_len   length of name argument
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_add_histogram_track_predicate(hxhim_options_t *opts, const char *name, const size_t name_len) {
    return hxhim_options_add_histogram_track_predicate(opts, std::string(name, name_len));
}

/**
 * hxhim_options_add_histogram_track_predicate
 *
 * @param opts       the set of options to be modified
 * @param name       the name of the predicate to track
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_add_histogram_track_predicate(hxhim_options_t *opts, const std::string &name) {
    if (!hxhim::valid(opts)) {
        return HXHIM_ERROR;
    }

    opts->p->histograms.names.insert(name);
    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_destroy
 * Destroys the contents of the hxhim_options_t, but not the variable itself
 *
 * @param opts the hxhim_options_t that contains the pointer to deallocate
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_destroy(hxhim_options_t *opts) {
    if ((hxhim_options_set_datastore(opts, nullptr) != HXHIM_SUCCESS) ||
        (hxhim_options_set_transport(opts, nullptr) != HXHIM_SUCCESS)) {
        return HXHIM_ERROR;
    }

    destruct(opts->p);
    opts->p = nullptr;

    return HXHIM_SUCCESS;
}
