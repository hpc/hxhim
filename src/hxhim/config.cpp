#include <sstream>
#include <vector>

#include "hxhim/config.h"
#include "hxhim/config.hpp"
#include "hxhim/constants.h"
#include "hxhim/options.h"
#include "hxhim/options_private.hpp"
#include "utils/Configuration.hpp"
#include "utils/Histogram.hpp"

/**
 * Individual functions for reading and parsing configuration values
 * They should be able to be run in any order without issue. If there
 * are config variable dependencies, they should all be handled within
 * a single function.
 *
 * This was done to clean up the fill_options function.
 *
 * @param opts the options to fill
 * @param the configuration to use
 * @return a boolean indicating whether or not an error was encountered
 */
static bool parse_debug_level(hxhim_options_t *opts, const Config &config) {
    int debug_level;
    const int ret = get_from_map(config, HXHIM_DEBUG_LEVEL, HXHIM_DEBUG_LEVELS, debug_level);
    if ((ret == CONFIG_ERROR)                                                                          ||
        ((ret == CONFIG_FOUND) && (hxhim_options_set_debug_level(opts, debug_level) != HXHIM_SUCCESS))) {
        return false;
    }

    return true;
}

static bool parse_datastore_count(hxhim_options_t *opts, const Config &config) {
    std::size_t datastores = 0;
    const int ret = get_value(config, HXHIM_DATASTORES_PER_RANGE_SERVER, datastores);
    if ((ret == CONFIG_ERROR)                                                                                         ||
        ((ret == CONFIG_FOUND) && (hxhim_options_set_datastores_per_range_server(opts, datastores) != HXHIM_SUCCESS))) {
        return false;
    }

    return true;
}

static bool parse_datastore(hxhim_options_t *opts, const Config &config) {
    hxhim_datastore_t datastore;

    int ret = get_from_map(config, HXHIM_DATASTORE_TYPE, HXHIM_DATASTORES, datastore);
    if (ret == CONFIG_ERROR) {
        return false;
    }

    if (ret == CONFIG_FOUND) {
        switch (datastore) {
            #if HXHIM_HAVE_LEVELDB
            case HXHIM_DATASTORE_LEVELDB:
                {
                    // get the leveldb datastore name prefix
                    Config_it name = config.find(HXHIM_LEVELDB_NAME);
                    if (name == config.end()) {
                        return false;
                    }

                    bool create_if_missing = true; // default to true; do not error if not found
                    if (get_bool(config, HXHIM_LEVELDB_CREATE_IF_MISSING, create_if_missing) == CONFIG_ERROR) {
                        return false;
                    }

                    int rank = -1;
                    if (MPI_Comm_rank(opts->p->comm, &rank) != MPI_SUCCESS) {
                        return false;
                    }
                    return (hxhim_options_set_datastore_leveldb(opts, rank, name->second.c_str(), create_if_missing) == HXHIM_SUCCESS);
                }
                break;
            #endif
            case HXHIM_DATASTORE_IN_MEMORY:
                return (hxhim_options_set_datastore_in_memory(opts) == HXHIM_SUCCESS);
            default:
                break;
        }
    }

    return false;
}

static bool parse_hash(hxhim_options_t *opts, const Config &config) {
    std::string hash;
    const int ret = get_value(config, HXHIM_HASH, hash);
    if ((ret == CONFIG_ERROR)                                                                         ||
        ((ret == CONFIG_FOUND) && (hxhim_options_set_hash_name(opts, hash.c_str()) != HXHIM_SUCCESS))) {
        return false;
    }

    return true;
}

static bool parse_transport(hxhim_options_t *opts, const Config &config) {
    Transport::Type transport_type;
    const int ret = get_from_map(config, HXHIM_TRANSPORT, HXHIM_TRANSPORTS, transport_type);
    if (ret == CONFIG_ERROR) {
        return false;
    }

    if (ret == CONFIG_FOUND) {
        switch (transport_type) {
            case Transport::TRANSPORT_NULL:
                {
                    return ((hxhim_options_set_transport_null(opts)     == HXHIM_SUCCESS) &&
                            (hxhim_options_set_hash_name(opts, "LOCAL") == HXHIM_SUCCESS));
                }
                break;
            case Transport::TRANSPORT_MPI:
                {
                    std::size_t listeners;
                    if ((get_value(config, HXHIM_MPI_LISTENERS, listeners) != CONFIG_FOUND)) {
                        return false;
                    }

                    return ((hxhim_options_set_transport_mpi(opts, listeners) == HXHIM_SUCCESS) &&
                            parse_hash(opts, config));
                }
                break;
            #if HXHIM_HAVE_THALLIUM
            case Transport::TRANSPORT_THALLIUM:
                {
                    Config_it thallium_module = config.find(HXHIM_THALLIUM_MODULE);
                    if (thallium_module == config.end()) {
                        return false;
                    }

                    return ((hxhim_options_set_transport_thallium(opts, thallium_module->second.c_str()) == HXHIM_SUCCESS) &&
                            parse_hash(opts, config));
                }
                break;
            #endif
            default:
                break;
        }
    }

    return false;
}

static bool parse_endpointgroup(hxhim_options_t *opts, const Config &config) {
    Config_it endpointgroup = config.find(HXHIM_TRANSPORT_ENDPOINT_GROUP);
    if (endpointgroup != config.end()) {
        hxhim_options_clear_endpoint_group(opts);
        if (endpointgroup->second == "ALL") {
            int size = -1;
            if (MPI_Comm_size(opts->p->comm, &size) != MPI_SUCCESS) {
                return false;
            }

            for(int rank = 0; rank < size; rank++) {
                hxhim_options_add_endpoint_to_group(opts, rank);
            }
            return true;
        }
        else {
            std::stringstream s(endpointgroup->second);
            int id;
            while (s >> id) {
                if (hxhim_options_add_endpoint_to_group(opts, id) != HXHIM_SUCCESS) {
                    // should probably write to mlog and continue instead of returning
                    return false;
                }
            }
        }
    }
    return false;
}

static bool parse_queued_bputs(hxhim_options_t *opts, const Config &config) {
    std::size_t queued_bputs = 0;
    const int ret = get_value(config, HXHIM_QUEUED_BULK_PUTS, queued_bputs);
    if ((ret == CONFIG_ERROR)                                                                          ||
        ((ret == CONFIG_FOUND) && hxhim_options_set_queued_bputs(opts, queued_bputs) != HXHIM_SUCCESS)) {
        return false;
    }

    return true;
}

static bool parse_histogram_first_n(hxhim_options_t *opts, const Config &config) {
    std::size_t use_first_n = 0;
    const int ret = get_value(config, HXHIM_HISTOGRAM_FIRST_N, use_first_n);
    if ((ret == CONFIG_ERROR)                                                                              ||
        ((ret == CONFIG_FOUND) && hxhim_options_set_histogram_first_n(opts, use_first_n) != HXHIM_SUCCESS)) {
        return false;
    }

    return true;
}

static bool parse_histogram_bucket_gen(hxhim_options_t *opts, const Config &config) {
    std::string method;
    const int ret = get_value(config, HXHIM_HISTOGRAM_BUCKET_GEN_METHOD, method);
    if ((ret == CONFIG_ERROR)                                                                                           ||
        ((ret == CONFIG_FOUND) && hxhim_options_set_histogram_bucket_gen_method(opts, method.c_str()) != HXHIM_SUCCESS)) {
        return false;
    }

    return true;
}

static bool parse_alloc_size(hxhim_options_t *opts, const Config &config, const std::string &key, int (*set_alloc_size)(hxhim_options_t *, std::size_t)) {
    std::size_t alloc_size = 0;
    const int ret = get_value(config, key, alloc_size);
    if ((ret == CONFIG_ERROR)                                                        ||
        ((ret == CONFIG_FOUND) && set_alloc_size(opts, alloc_size) != HXHIM_SUCCESS)) {
        return false;
    }

    return true;
}

static bool parse_regions(hxhim_options_t *opts, const Config &config, const std::string &key, int (*set_regions)(hxhim_options_t *, std::size_t)) {
    std::size_t regions = 0;
    const int ret = get_value(config, key, regions);
    if ((ret == CONFIG_ERROR)                                                  ||
        ((ret == CONFIG_FOUND) && set_regions(opts, regions) != HXHIM_SUCCESS)) {
        return false;
    }

    return true;
}

static bool parse_name(hxhim_options_t *opts, const Config &config, const std::string &key, int (*set_name)(hxhim_options_t *, const char *)) {
    std::string name = "";
    const int ret = get_value(config, key, name);
    if ((ret == CONFIG_ERROR)                                                    ||
        ((ret == CONFIG_FOUND) && set_name(opts, name.c_str()) != HXHIM_SUCCESS)) {
        return false;
    }

    return true;
}

/**
 * fill_options
 * Fills up opts as best it can using config.
 * The config can be incomplete, so long as each
 * set of configuration variables is complete.
 * New values will overwrite old values.
 *
 * @param opts the options to fill
 * @param the configuration to use
 * @param HXHIM_SUCCESS or HXHIM_ERROR on error
 */
static int fill_options(hxhim_options_t *opts, const Config &config) {
    if (!opts || !opts->p                ||
        (opts->p->comm == MPI_COMM_NULL)) {
        return HXHIM_ERROR;
    }

    return
        parse_debug_level(opts, config) &&
        parse_datastore_count(opts, config) &&
        parse_datastore(opts, config) &&
        parse_transport(opts, config) &&
        parse_endpointgroup(opts, config) &&
        parse_queued_bputs(opts, config) &&
        parse_histogram_first_n(opts, config) &&
        parse_histogram_bucket_gen(opts, config) &&
        parse_name      (opts, config, HXHIM_PACKED_NAME,          hxhim_options_set_packed_name) &&
        parse_alloc_size(opts, config, HXHIM_PACKED_ALLOC_SIZE,    hxhim_options_set_packed_alloc_size) &&
        parse_regions   (opts, config, HXHIM_PACKED_REGIONS,       hxhim_options_set_packed_regions) &&
        parse_name      (opts, config, HXHIM_BUFFERS_NAME,         hxhim_options_set_buffers_name) &&
        parse_alloc_size(opts, config, HXHIM_BUFFERS_ALLOC_SIZE,   hxhim_options_set_buffers_alloc_size) &&
        parse_regions   (opts, config, HXHIM_BUFFERS_REGIONS,      hxhim_options_set_buffers_regions) &&
        parse_name      (opts, config, HXHIM_BULKS_NAME,           hxhim_options_set_bulks_name) &&
        // parse_alloc_size(opts, config, HXHIM_BULKS_ALLOC_SIZE,     hxhim_options_set_bulks_alloc_size) &&
        parse_regions   (opts, config, HXHIM_BULKS_REGIONS,        hxhim_options_set_bulks_regions) &&
        parse_name      (opts, config, HXHIM_KEYS_NAME,            hxhim_options_set_keys_name) &&
        parse_alloc_size(opts, config, HXHIM_KEYS_ALLOC_SIZE,      hxhim_options_set_keys_alloc_size) &&
        parse_regions   (opts, config, HXHIM_KEYS_REGIONS,         hxhim_options_set_keys_regions) &&
        parse_name      (opts, config, HXHIM_ARRAYS_NAME,          hxhim_options_set_arrays_name) &&
        parse_alloc_size(opts, config, HXHIM_ARRAYS_ALLOC_SIZE,    hxhim_options_set_arrays_alloc_size) &&
        parse_regions   (opts, config, HXHIM_ARRAYS_REGIONS,       hxhim_options_set_arrays_regions) &&
        parse_name      (opts, config, HXHIM_REQUESTS_NAME,        hxhim_options_set_requests_name) &&
        // parse_alloc_size(opts, config, HXHIM_REQUESTS_ALLOC_SIZE,  hxhim_options_set_requests_alloc_size) &&
        parse_regions   (opts, config, HXHIM_REQUESTS_REGIONS,     hxhim_options_set_requests_regions) &&
        parse_name      (opts, config, HXHIM_RESPONSES_NAME,       hxhim_options_set_responses_name) &&
        // parse_alloc_size(opts, config, HXHIM_RESPONSES_ALLOC_SIZE, hxhim_options_set_responses_alloc_size) &&
        parse_regions   (opts, config, HXHIM_RESPONSES_REGIONS,    hxhim_options_set_responses_regions) &&
        parse_name      (opts, config, HXHIM_RESULT_NAME,          hxhim_options_set_result_name) &&
        // parse_alloc_size(opts, config, HXHIM_RESULT_ALLOC_SIZE,    hxhim_options_set_result_alloc_size) &&
        parse_regions   (opts, config, HXHIM_RESULT_REGIONS,       hxhim_options_set_result_regions) &&
        parse_name      (opts, config, HXHIM_RESULTS_NAME,         hxhim_options_set_results_name) &&
        // parse_alloc_size(opts, config, HXHIM_RESULTS_ALLOC_SIZE,   hxhim_options_set_results_alloc_size) &&
        // parse_regions   (opts, config, HXHIM_RESULTS_REGIONS,      hxhim_options_set_results_regions) &&
        true?HXHIM_SUCCESS:HXHIM_ERROR;
}

/**
 * process_config_and_fill_options
 * This function is just a logical separator between
 * setting up the configuration reader and filling out
 * the hxhim_t.
 *
 * This function should be common for all configuration readers.
 *
 * This function should not be modified unless the
 * ConfigReader interface changes.
 *
 * opts should be cleaned up by the calling function.
 *
 * @param opts            the options to fill
 * @param config_sequence the configuration reader that has been set up
 * @return whether or not opts was successfully filled
 */
int process_config_and_fill_options(hxhim_options_t *opts, ConfigSequence &config_sequence) {
    if (!opts || !opts->p) {
        return HXHIM_ERROR;
    }

    // Parse the configuration data
    Config config;
    config_sequence.process(config);

    // fill opts with values from configuration
    return fill_options(opts, config);
}

/**
 * hxhim_default_config_reader
 * This function acts as both the default
 * configuration reader as well as an example
 * of how custom configuration readers should
 * be implmented.
 *
 * @param opts the options to fill
 * @return whether or not configuration was completed
 */
int hxhim_default_config_reader(hxhim_options_t *opts, MPI_Comm comm) {
    if (!opts) {
        return HXHIM_ERROR;
    }

    ConfigSequence config_sequence;

    // add default search locations in order of preference: default filename, file environment variable, environment variable overrides
    ConfigFile file(HXHIM_CONFIG_FILE);
    config_sequence.add(&file);

    ConfigFileEnvironment fileenv(HXHIM_CONFIG_ENV);
    config_sequence.add(&fileenv);

    // get the environment variables from the default configuration
    std::vector<ConfigVarEnvironment *> vars;
    for(decltype(HXHIM_DEFAULT_CONFIG)::value_type const &default_config : HXHIM_DEFAULT_CONFIG) {
        ConfigVarEnvironment *var = new ConfigVarEnvironment(default_config.first);
        config_sequence.add(var);
        vars.push_back(var);
    }

    int ret = HXHIM_SUCCESS;

    hxhim_options_init(opts);
    hxhim_options_set_mpi_bootstrap(opts, comm);

    if ((fill_options(opts, HXHIM_DEFAULT_CONFIG)               != HXHIM_SUCCESS) || // fill in the options with default values
        (process_config_and_fill_options(opts, config_sequence) != HXHIM_SUCCESS)) { // read the configuration and overwrite default values
        ret = HXHIM_ERROR;
    }

    for(ConfigVarEnvironment *&var : vars) {
        delete var;
    }

    return ret;
}
