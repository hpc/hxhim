#include <sstream>
#include <vector>

#include "datastore/transform.hpp"
#include "hxhim/config.hpp"
#include "hxhim/constants.h"
#include "hxhim/options.h"
#include "hxhim/options.hpp"
#include "hxhim/private/options.hpp"
#include "utils/Histogram.hpp"
#include "utils/type_traits.hpp"

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
bool parse_value(hxhim_options_t *opts, const Config::Config &config, const std::string &key, int (*set_option)(hxhim_options_t *, const T)) {
    T value = {};
    const int ret = Config::get_value(config, key, value);
    if ((ret == Config::ERROR)                                                 ||
        ((ret == Config::FOUND) && (set_option(opts, value) != HXHIM_SUCCESS))) {
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
static bool parse_value(hxhim_options_t *opts, const Config::Config &config, const std::string &key, int (*set_option)(hxhim_options_t *, const char *)) {
    std::string value;
    const int ret = Config::get_value(config, key, value);
    if ((ret == Config::ERROR)                                                         ||
        ((ret == Config::FOUND) && (set_option(opts, value.c_str()) != HXHIM_SUCCESS))) {
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
bool parse_map_value(hxhim_options_t *opts, const Config::Config &config, const std::string &key, const std::unordered_map<std::string, T> &map, int (*set_option)(hxhim_options_t *, const T)) {
    T value = {};
    const int ret = Config::get_from_map(config, key, map, value);
    if ((ret == Config::ERROR)                                                 ||
        ((ret == Config::FOUND) && (set_option(opts, value) != HXHIM_SUCCESS))) {
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
static bool parse_datastore(hxhim_options_t *opts, const Config::Config &config) {
    datastore::Type datastore;

    int ret = Config::get_from_map(config, hxhim::config::DATASTORE_TYPE, hxhim::config::DATASTORES, datastore);

    if (ret == Config::NOT_FOUND) {
        return false;
    }

    if (ret == Config::ERROR) {
        return false;
    }

    if (ret == Config::FOUND) {
        switch (datastore) {
            case datastore::IN_MEMORY:
                return (hxhim_options_set_datastore_in_memory(opts) == HXHIM_SUCCESS);
            #if HXHIM_HAVE_LEVELDB
            case datastore::LEVELDB:
                {
                    // get the leveldb datastore prefix prefix
                    Config::Config_it prefix = config.find(hxhim::config::LEVELDB_PREFIX);
                    if (prefix == config.end()) {
                        return false;
                    }

                    bool create_if_missing = true; // default to true; do not error if not found
                    if (Config::get_value(config, hxhim::config::LEVELDB_CREATE_IF_MISSING, create_if_missing) == Config::ERROR) {
                        return false;
                    }

                    return (hxhim_options_set_datastore_leveldb(opts, prefix->second.c_str(), create_if_missing) == HXHIM_SUCCESS);
                }
                break;
            #endif
            #if HXHIM_HAVE_ROCKSDB
            case datastore::ROCKSDB:
                {
                    // get the rocksdb datastore prefix prefix
                    Config::Config_it prefix = config.find(hxhim::config::ROCKSDB_PREFIX);
                    if (prefix == config.end()) {
                        return false;
                    }

                    bool create_if_missing = true; // default to true; do not error if not found
                    if (Config::get_value(config, hxhim::config::ROCKSDB_CREATE_IF_MISSING, create_if_missing) == Config::ERROR) {
                        return false;
                    }

                    return (hxhim_options_set_datastore_rocksdb(opts, prefix->second.c_str(), create_if_missing) == HXHIM_SUCCESS);
                }
                break;
            #endif
            // should never get here
            default:
                return false;
        }
    }

    // should never get here
    return false;
}

static bool parse_hash(hxhim_options_t *opts, const Config::Config &config) {
    return parse_value(opts, config, hxhim::config::HASH, hxhim_options_set_hash_name);
}

static bool parse_transport(hxhim_options_t *opts, const Config::Config &config) {
    Transport::Type transport_type;
    const int ret = Config::get_from_map(config, hxhim::config::TRANSPORT, hxhim::config::TRANSPORTS, transport_type);
    if (ret == Config::ERROR) {
        return false;
    }

    if (ret == Config::FOUND) {
        switch (transport_type) {
            case Transport::TRANSPORT_NULL:
                {
                    return ((hxhim_options_set_transport_null(opts)                     == HXHIM_SUCCESS) &&
                            (hxhim_options_set_hash_name(opts, "RANK_MOD_RANGESERVERS") == HXHIM_SUCCESS));
                }
                break;
            case Transport::TRANSPORT_MPI:
                {
                    std::size_t listeners;
                    if (Config::get_value(config, hxhim::config::MPI_LISTENERS, listeners) != Config::FOUND) {
                        return false;
                    }

                    return ((hxhim_options_set_transport_mpi(opts, listeners) == HXHIM_SUCCESS) &&
                           parse_hash(opts, config));
                }
                break;
            #if HXHIM_HAVE_THALLIUM
            case Transport::TRANSPORT_THALLIUM:
                {
                    Config::Config_it thallium_module = config.find(hxhim::config::THALLIUM_MODULE);
                    if (thallium_module == config.end()) {
                        return false;
                    }

                    return ((hxhim_options_set_transport_thallium(opts, thallium_module->second) == HXHIM_SUCCESS) &&
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

static bool parse_endpointgroup(hxhim_options_t *opts, const Config::Config &config) {
    Config::Config_it endpointgroup = config.find(hxhim::config::TRANSPORT_ENDPOINT_GROUP);
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
            int id;
            while (std::stringstream(endpointgroup->second) >> id) {
                if (hxhim_options_add_endpoint_to_group(opts, id) != HXHIM_SUCCESS) {
                    // should probably write to mlog and continue instead of returning
                    return false;
                }
            }
        }
    }
    return false;
}

static bool parse_elen(hxhim_options_t *opts, const Config::Config &config) {
    char neg = elen::NEG_SYMBOL;
    char pos = elen::POS_SYMBOL;
    int  flt_precision = elen::encode::FLOAT_PRECISION;
    int  dbl_precision = elen::encode::DOUBLE_PRECISION;

    Config::Config_it neg_it = config.find(hxhim::config::ELEN_NEG_SYMBOL);
    if (neg_it != config.end()) {
        if (!(std::stringstream(neg_it->second) >> neg)) {
            return false;
        }
    }

    Config::Config_it pos_it = config.find(hxhim::config::ELEN_POS_SYMBOL);
    if (pos_it != config.end()) {
        if (!(std::stringstream(pos_it->second) >> pos)) {
            return false;
        }
    }

    Config::Config_it flt_it = config.find(hxhim::config::ELEN_ENCODE_FLOAT_PRECISION);
    if (flt_it != config.end()) {
        if (!(std::stringstream(flt_it->second) >> flt_precision)) {
            return false;
        }
    }

    Config::Config_it dbl_it = config.find(hxhim::config::ELEN_ENCODE_DOUBLE_PRECISION);
    if (dbl_it != config.end()) {
        if (!(std::stringstream(dbl_it->second) >> dbl_precision)) {
            return false;
        }
    }

    if (neg >= pos) {
        return false;
    }

    if (flt_precision >= dbl_precision) {
        return false;
    }

    return (hxhim_options_set_transform_numeric_values(opts, neg, pos, flt_precision, dbl_precision) == HXHIM_SUCCESS);
}

static bool parse_histogram(hxhim_options_t *opt, const Config::Config &config) {
    if (!parse_value(opt, config, hxhim::config::HISTOGRAM_FIRST_N,          hxhim_options_set_histogram_first_n)         ||
        !parse_value(opt, config, hxhim::config::HISTOGRAM_BUCKET_GEN_NAME,  hxhim_options_set_histogram_bucket_gen_name)) {
        return false;
    }

    bool read = true; // default to true; do not error if not found
    if (Config::get_value(config, hxhim::config::HISTOGRAM_READ_EXISTING, read) == Config::ERROR) {
        return false;
    }

    bool write = true; // default to true; do not error if not found
    if (Config::get_value(config, hxhim::config::HISTOGRAM_WRITE_AT_EXIT, write) == Config::ERROR) {
        return false;
    }

    if (hxhim_options_datastore_histograms(opt, read, write) != HXHIM_SUCCESS) {
        return false;
    }

    Config::Config_it hist_name_it = config.find(hxhim::config::HISTOGRAM_TRACK_PREDICATES);
    if (hist_name_it != config.end()) {
        std::stringstream s(hist_name_it->second);
        std::string name;
        while (std::getline(s, name, ',')) {
            opt->p->histograms.names.emplace(name);
        }
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
static int fill_options(hxhim_options_t *opts, const Config::Config &config) {
    if (!opts || !opts->p                ||
        (opts->p->comm == MPI_COMM_NULL)) {
        return HXHIM_ERROR;
    }

    using namespace hxhim::config;

    return
        parse_map_value(opts, config, DEBUG_LEVEL, DEBUG_LEVELS, hxhim_options_set_debug_level)                 &&
        parse_value(opts, config, CLIENT_RATIO,                  hxhim_options_set_client_ratio)                &&
        parse_value(opts, config, SERVER_RATIO,                  hxhim_options_set_server_ratio)                &&
        parse_datastore(opts, config)                                                                           &&
        parse_transport(opts, config)                                                                           &&
        parse_endpointgroup(opts, config)                                                                       &&
        parse_value(opts, config, START_ASYNC_PUT_AT,            hxhim_options_set_start_async_put_at)          &&
        parse_value(opts, config, MAXIMUM_OPS_PER_SEND,          hxhim_options_set_maximum_ops_per_send)        &&
        parse_elen(opts, config)                                                                                &&
        parse_histogram(opts, config)                                                                           &&
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
 * @param opts     the options to fill
 * @param sequence the configuration reader that has been set up
 * @return whether or not opts was successfully filled
 */
int process_config_and_fill_options(hxhim_options_t *opts, Config::Sequence &sequence) {
    if (!opts || !opts->p) {
        return HXHIM_ERROR;
    }

    // Parse the configuration data
    Config::Config config;
    sequence.process(config);

    // fill opts with values from configuration
    return fill_options(opts, config);
}

/**
 * default_reader
 * This function acts as both the default
 * configuration reader as well as an example
 * of how custom configuration readers should
 * be implmented.
 *
 * @param opts the options to fill
 * @return whether or not configuration was completed
 */
int hxhim::config::default_reader(hxhim_options_t *opts, MPI_Comm comm) {
    if (!opts) {
        return HXHIM_ERROR;
    }

    Config::Sequence sequence;

    // try to find the default configuration file first
    Config::File file(hxhim::config::CONFIG_FILE);
    sequence.add(&file);

    // overwrite values with the file pointed to by Config::ENV
    Config::EnvironmentFile fileenv(hxhim::config::CONFIG_ENV);
    sequence.add(&fileenv);

    // overwite values with individual environment variables
    std::vector<Config::EnvironmentVar *> vars;
    for(decltype(hxhim::config::DEFAULT_CONFIG)::value_type const &default_config : hxhim::config::DEFAULT_CONFIG) {
        Config::EnvironmentVar *var = new Config::EnvironmentVar(hxhim::config::HXHIM_ENV_NAMESPACE + default_config.first, default_config.first);
        sequence.add(var);
        vars.push_back(var);
    }

    int ret = HXHIM_SUCCESS;

    hxhim_options_init(opts);
    hxhim_options_set_mpi_bootstrap(opts, comm);

    if ((fill_options(opts, hxhim::config::DEFAULT_CONFIG) != HXHIM_SUCCESS) || // fill in the options with default values
        (process_config_and_fill_options(opts, sequence)   != HXHIM_SUCCESS)) { // read the configuration and overwrite default values
        ret = HXHIM_ERROR;
    }

    for(Config::EnvironmentVar *var : vars) {
        delete var;
    }

    return ret;
}

/**
 * hxhim_config_default_reader
 * This function acts as both the default
 * configuration reader as well as an example
 * of how custom configuration readers should
 * be implmented.
 *
 * @param opts the options to fill
 * @return whether or not configuration was completed
 */
int hxhim_config_default_reader(hxhim_options_t *opts, MPI_Comm comm) {
    return hxhim::config::default_reader(opts, comm);
}
