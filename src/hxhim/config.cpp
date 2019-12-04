#include <sstream>
#include <vector>

#include "hxhim/Results.hpp"
#include "hxhim/config.h"
#include "hxhim/config.hpp"
#include "hxhim/constants.h"
#include "hxhim/options.h"
#include "hxhim/options.hpp"
#include "hxhim/options_private.hpp"
#include "utils/enable_if_t.hpp"
#include "utils/Histogram.hpp"

/**
 * parse_value
 * This function template is a helper function
 * for parsing and setting simple values
 *
 * @param opts        the options struct being built
 * @param config      the configuration
 * @param key         the key within the configuration to look for
 * @param set_option  the function to do the setting of the option
 * @param true, or false on error
 */
template <typename T, typename = enable_if_t <!std::is_reference<T>::value> >
bool parse_value(hxhim_options_t *opts, const Config &config, const std::string &key, int (*set_option)(hxhim_options_t *, const T)) {
    T value = {};
    const int ret = get_value(config, key, value);
    if ((ret == CONFIG_ERROR)                                                 ||
        ((ret == CONFIG_FOUND) && (set_option(opts, value) != HXHIM_SUCCESS))) {
        return false;
    }

    return true;
}

/**
 * parse_value
 * This function template is a helper function for
 * parsing and setting char * configuration values
 *
 * @param opts        the options struct being built
 * @param config      the configuration
 * @param key         the key within the configuration to look for
 * @param set_option  the function to do the setting of the option
 * @param true, or false on error
 */
static bool parse_value(hxhim_options_t *opts, const Config &config, const std::string &key, int (*set_option)(hxhim_options_t *, const char *)) {
    std::string value;
    const int ret = get_value(config, key, value);
    if ((ret == CONFIG_ERROR)                                                         ||
        ((ret == CONFIG_FOUND) && (set_option(opts, value.c_str()) != HXHIM_SUCCESS))) {
        return false;
    }

    return true;
}

/**
 * parse_map_value
 * This function template is a helper function
 * for parsing and setting values that are mapped
 *
 * @param opts        the options struct being built
 * @param config      the configuration
 * @param key         the key within the configuration to look for
 * @param map         the map to search the key for
 * @param set_option  the function to do the setting of the option
 * @param true, or false on error
 */
template <typename T, typename = enable_if_t <!std::is_reference<T>::value> >
bool parse_map_value(hxhim_options_t *opts, const Config &config, const std::string &key, const std::unordered_map<std::string, T> &map, int (*set_option)(hxhim_options_t *, const T)) {
    T value = {};
    const int ret = get_from_map(config, key, map, value);
    if ((ret == CONFIG_ERROR)                                                 ||
        ((ret == CONFIG_FOUND) && (set_option(opts, value) != HXHIM_SUCCESS))) {
        return false;
    }

    return true;
}

/**
 * Individual functions for reading and parsing configuration values
 * They should be able to be run in any order without issue. If there
 * are config variable dependencies, they should all be handled within
 * a single function.
 *
 * This was done to clean up the fill_options function.
 *
 * @param opts the options to fill
 * @param config the configuration to use
 * @return a boolean indicating whether or not an error was encountered
 */
static bool parse_datastore(hxhim_options_t *opts, const Config &config) {
    hxhim::datastore::Type datastore;

    int ret = get_from_map(config, hxhim::config::DATASTORE_TYPE, hxhim::config::DATASTORES, datastore);
    if (ret == CONFIG_ERROR) {
        return false;
    }

    if (ret == CONFIG_FOUND) {
        switch (datastore) {
            #if HXHIM_HAVE_LEVELDB
            case hxhim::datastore::LEVELDB:
                {
                    // get the leveldb datastore prefix prefix
                    Config_it prefix = config.find(hxhim::config::LEVELDB_PREFIX);
                    if (prefix == config.end()) {
                        return false;
                    }

                    bool create_if_missing = true; // default to true; do not error if not found
                    if (get_bool(config, hxhim::config::LEVELDB_CREATE_IF_MISSING, create_if_missing) == CONFIG_ERROR) {
                        return false;
                    }

                    int rank = -1;
                    if (MPI_Comm_rank(opts->p->comm, &rank) != MPI_SUCCESS) {
                        return false;
                    }
                    return (hxhim_options_set_datastore_leveldb(opts, rank, prefix->second.c_str(), create_if_missing) == HXHIM_SUCCESS);
                }
                break;
            #endif
            case hxhim::datastore::IN_MEMORY:
                return (hxhim_options_set_datastore_in_memory(opts) == HXHIM_SUCCESS);
            default:
                break;
        }
    }

    return false;
}

static bool parse_hash(hxhim_options_t *opts, const Config &config) {
    return parse_value(opts, config, hxhim::config::HASH, hxhim_options_set_hash_name);
}

static bool parse_transport(hxhim_options_t *opts, const Config &config) {
    Transport::Type transport_type;
    const int ret = get_from_map(config, hxhim::config::TRANSPORT, hxhim::config::TRANSPORTS, transport_type);
    if (ret == CONFIG_ERROR) {
        return false;
    }

    if (ret == CONFIG_FOUND) {
        switch (transport_type) {
            case Transport::TRANSPORT_NULL:
                {
                    return ((hxhim_options_set_transport_null(opts)       == HXHIM_SUCCESS) &&
                            (hxhim_options_set_hash_name(opts, "MY_RANK") == HXHIM_SUCCESS));
                }
                break;
            case Transport::TRANSPORT_MPI:
                {
                    std::size_t listeners;
                    if (get_value(config, hxhim::config::MPI_LISTENERS, listeners) != CONFIG_FOUND) {
                        return false;
                    }

                    return ((hxhim_options_set_transport_mpi(opts, listeners) == HXHIM_SUCCESS) &&
                           parse_hash(opts, config));
                }
                break;
            #if HXHIM_HAVE_THALLIUM
            case Transport::TRANSPORT_THALLIUM:
                {
                    Config_it thallium_module = config.find(hxhim::config::THALLIUM_MODULE);
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
    Config_it endpointgroup = config.find(hxhim::config::TRANSPORT_ENDPOINT_GROUP);
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

    using namespace hxhim::config;

    return
        parse_map_value(opts, config, DEBUG_LEVEL, DEBUG_LEVELS, hxhim_options_set_debug_level)                 &&
        parse_value(opts, config, CLIENT_RATIO,                  hxhim_options_set_client_ratio)                &&
        parse_value(opts, config, SERVER_RATIO,                  hxhim_options_set_server_ratio)                &&
        parse_value(opts, config, DATASTORES_PER_RANGE_SERVER,   hxhim_options_set_datastores_per_range_server) &&
        parse_datastore(opts, config)                                                                           &&
        parse_transport(opts, config)                                                                           &&
        parse_endpointgroup(opts, config)                                                                       &&
        parse_value(opts, config, START_ASYNC_PUT_AT,            hxhim_options_set_start_async_put_at)          &&
        parse_value(opts, config, MAXIMUM_OPS_PER_SEND,          hxhim_options_set_maximum_ops_per_send)        &&
        parse_value(opts, config, HISTOGRAM_FIRST_N,             hxhim_options_set_histogram_first_n)           &&
        parse_value(opts, config, HISTOGRAM_BUCKET_GEN_METHOD,   hxhim_options_set_histogram_bucket_gen_method) &&
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
    ConfigFile file(hxhim::config::CONFIG_FILE);
    config_sequence.add(&file);

    ConfigFileEnvironment fileenv(hxhim::config::CONFIG_ENV);
    config_sequence.add(&fileenv);

    // get the environment variables from the default configuration
    std::vector<ConfigVarEnvironment *> vars;
    for(decltype(hxhim::config::DEFAULT_CONFIG)::value_type const &default_config : hxhim::config::DEFAULT_CONFIG) {
        ConfigVarEnvironment *var = new ConfigVarEnvironment(default_config.first);
        config_sequence.add(var);
        vars.push_back(var);
    }

    int ret = HXHIM_SUCCESS;

    hxhim_options_init(opts);
    hxhim_options_set_mpi_bootstrap(opts, comm);

    if ((fill_options(opts, hxhim::config::DEFAULT_CONFIG)      != HXHIM_SUCCESS) || // fill in the options with default values
        (process_config_and_fill_options(opts, config_sequence) != HXHIM_SUCCESS)) { // read the configuration and overwrite default values
        ret = HXHIM_ERROR;
    }

    for(ConfigVarEnvironment *var : vars) {
        delete var;
    }

    return ret;
}
