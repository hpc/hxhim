#include "hxhim/config.hpp"
#include "hxhim/options.h"
#include "hxhim/options_private.hpp"
#include "transport/backend/MPI/Options.hpp"
#include "transport/backend/Thallium/Options.hpp"

/**
 * hxhim_options_init
 * Allocates the private pointer of an existing hxhim_options_t structure
 *
 * @param opts address of an existing hxhim_options_t structure
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_init(hxhim_options_t *opts) {
    if (!opts) {
        return HXHIM_ERROR;
    }

    if (!(opts->p = new hxhim_options_private_t())) {
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

/**
 * valid_opts
 *
 * @param opts
 * @return whether or not opts and opts->p are both non NULL
 */
static bool valid_opts(hxhim_options_t *opts) {
    return (opts && opts->p);
}

/**
 * hxhim_options_set_mpi_bootstrap
 *
 * @param opts the set of options to be modified
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_mpi_bootstrap(hxhim_options_t *opts, MPI_Comm comm) {
    if (!valid_opts(opts)) {
        return HXHIM_ERROR;
    }

    return ((opts->p->comm = comm) == MPI_COMM_NULL)?HXHIM_ERROR:HXHIM_SUCCESS;
}

int hxhim_options_set_datastores_per_range_server(hxhim_options_t *opts, const size_t count) {
    if (!valid_opts(opts)) {
        return HXHIM_ERROR;
    }

    opts->p->datastore_count = count;

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
static int hxhim_options_set_datastore(hxhim_options_t *opts, hxhim_datastore_config_t *config) {
    if (!valid_opts(opts)) {
        return HXHIM_ERROR;
    }

    hxhim_options_datastore_config_destroy(opts->p->datastore);

    opts->p->datastore = config;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_datastore_leveldb
 * Sets up the values needed for a leveldb datastore
 *
 * @param opts   the set of options to be modified
 * @param path the name prefix for each datastore
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_datastore_leveldb(hxhim_options_t *opts, const size_t id, const char *path, const int create_if_missing) {
    hxhim_datastore_config_t *config = hxhim_options_create_leveldb_config(id, path, create_if_missing);
    if (!config) {
        return HXHIM_ERROR;
    }

    if (hxhim_options_set_datastore(opts, config) != HXHIM_SUCCESS) {
        delete config;
        return HXHIM_ERROR;
    }
    return HXHIM_SUCCESS;
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
    hxhim_datastore_config_t *config = hxhim_options_create_in_memory_config();
    if (!config) {
        return HXHIM_ERROR;
    }

    if (hxhim_options_set_datastore(opts, config) != HXHIM_SUCCESS) {
        delete config;
        return HXHIM_ERROR;
    }
    return HXHIM_SUCCESS;
}

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
int hxhim_options_set_hash_name(hxhim_options_t *opts, const char *hash) {
    if (!valid_opts(opts)) {
        return HXHIM_ERROR;
    }

    std::map<std::string, hxhim_hash_t>::const_iterator it = HXHIM_HASHES.find(hash);
    if (it == HXHIM_HASHES.end()) {
        return HXHIM_ERROR;
    }

    opts->p->hash = it->second;
    opts->p->hash_args = nullptr;

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
int hxhim_options_set_hash_function(hxhim_options_t *opts, hxhim_hash_t hash, void *args) {
    if (!valid_opts(opts)) {
        return HXHIM_ERROR;
    }

    if (!hash) {
        return HXHIM_ERROR;
    }

    opts->p->hash = hash;
    opts->p->hash_args = nullptr;

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
    if (!valid_opts(opts)) {
        return HXHIM_ERROR;
    }

    delete opts->p->transport;

    opts->p->transport = config;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_transport_mpi
 * Sets the values needed to set up a mpi Transport
 *
 * @param opts              the set of options to be modified
 * @param memory_alloc_size the size of each region of memory
 * @param memory_regions    the number of memory regions
 * @param listeners         the number of listeners
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_transport_mpi(hxhim_options_t *opts, const size_t memory_alloc_size, const size_t memory_regions, const size_t listeners) {
    if (!opts || !opts->p) {
        return HXHIM_ERROR;
    }

    Transport::Options *config = new Transport::MPI::Options(opts->p->comm, memory_alloc_size, memory_regions, listeners);
    if (!config) {
        return HXHIM_ERROR;
    }

    if (hxhim_options_set_transport(opts, config) != HXHIM_SUCCESS) {
        delete config;
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
    Transport::Options *config = new Transport::Thallium::Options(module);
    if (!config) {
        return HXHIM_ERROR;
    }

    if (hxhim_options_set_transport(opts, config) != HXHIM_SUCCESS) {
        delete config;
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_add_endpoint_to_group
 * Adds an endpoint to the endpoint group
 *
 * @param opts   the set of options to be modified
 * @param id     the unique id to add
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_add_endpoint_to_group(hxhim_options_t *opts, const int id) {
    if (!valid_opts(opts)) {
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
    if (!valid_opts(opts)) {
        return HXHIM_ERROR;
    }

    opts->p->endpointgroup.clear();
    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_queued_puts
 * Set the number of bulk PUTs to queue up before flushing in the background thread
 *
 * @param opts  the set of options to be modified
 * @param count the number of PUTs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_queued_bputs(hxhim_options_t *opts, const std::size_t count) {
    if (!valid_opts(opts)) {
        return HXHIM_ERROR;
    }

    opts->p->queued_bputs = count;

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
    if (!valid_opts(opts)) {
        return HXHIM_ERROR;
    }

    opts->p->histogram.first_n = count;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_histogram_bucket_gen_method
 * Set the name of the bucket generation method.
 * This function only allows for predefined functions
 * because the function signature uses C++ objects
 *
 * @param opts   the set of options to be modified
 * @param method the name of the method
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_histogram_bucket_gen_method(hxhim_options_t *opts, const char *method) {
    if (!valid_opts(opts)) {
        return HXHIM_ERROR;
    }

    decltype(HXHIM_HISTOGRAM_BUCKET_GENERATORS)::const_iterator gen_it = HXHIM_HISTOGRAM_BUCKET_GENERATORS.find(method);
    if (gen_it == HXHIM_HISTOGRAM_BUCKET_GENERATORS.end()) {
        return HXHIM_ERROR;
    }

    decltype(HXHIM_HISTOGRAM_BUCKET_GENERATOR_EXTRA_ARGS)::const_iterator args_it = HXHIM_HISTOGRAM_BUCKET_GENERATOR_EXTRA_ARGS.find(method);
    if (args_it == HXHIM_HISTOGRAM_BUCKET_GENERATOR_EXTRA_ARGS.end()) {
        return HXHIM_ERROR;
    }

    opts->p->histogram.gen = gen_it->second;
    opts->p->histogram.args = args_it->second;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_histogram_bucket_gen_method
 * Set the name of the bucket generation method.
 * This function allows for arbitrary functions
 * that match the histogram bucket generator function
 * signature to be used.
 *
 * @param opts   the set of options to be modified
 * @param gen    the custom function for generating buckets
 * @param args   arguments the custom function will use at runtime
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_histogram_bucket_gen_method(hxhim_options_t *opts, const Histogram::BucketGen::generator &gen, void *args) {
    if (!valid_opts(opts)) {
        return HXHIM_ERROR;
    }

    if (!gen) {
        return HXHIM_ERROR;
    }

    opts->p->histogram.gen = gen;
    opts->p->histogram.args = args;

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
    if (!opts) {
        return HXHIM_ERROR;
    }

    hxhim_options_datastore_config_destroy(opts->p->datastore);
    delete opts->p->transport;

    delete opts->p;
    opts->p = nullptr;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_create_leveldb_config
 *
 * @param path               the name prefix for each datastore
 * @param create_if_missing  whether or not leveldb should create new datastores if the datastores do not already exist
 * @return a pointer to the configuration data, or a nullptr
 */
hxhim_datastore_config_t *hxhim_options_create_leveldb_config(const size_t id, const char *path, const int create_if_missing) {
    hxhim_leveldb_config_t *config = new hxhim_leveldb_config_t();
    if (config) {
        config->id = id;
        config->path = path;
        config->create_if_missing = create_if_missing;
    }
    return config;
}

/**
 * hxhim_options_create_in_memory_config
 *
 * @return a pointer to the configuration data, or a nullptr
 */
hxhim_datastore_config_t *hxhim_options_create_in_memory_config() {
    return new hxhim_in_memory_config_t();
}

// Cleans up config memory, including the config variable itself because the user will never be able to create their own config
void hxhim_options_datastore_config_destroy(hxhim_datastore_config_t *config) {
    delete config;
}
