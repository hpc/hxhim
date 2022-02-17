#include <iostream>
#include <sstream>
#include <vector>

#include "datastore/transform.hpp"
#include "hxhim/config.hpp"
#include "hxhim/constants.h"
#include "hxhim/options.hpp"
#include "hxhim/private/hxhim.hpp"
#include "utils/Histogram.hpp"
#include "utils/type_traits.hpp"

/**
 * parse_value
 * This function template is a helper function
 * for parsing and setting simple values
 *
 * @param hx          the hxhim instance being built
 * @param config      the configuration
 * @param key         the key within the configuration to look for
 * @param set_option  the function to do the setting of the option
 * @param true, or false on error
 */
template <typename T, typename = enable_if_t <!std::is_reference<T>::value> >
bool parse_value(hxhim_t *hx, const Config::Config &config, const std::string &key, int (*set_option)(hxhim_t *, const T)) {
    T value = {};
    const int ret = Config::get_value(config, key, value);
    if ((ret == Config::ERROR)                                               ||
        ((ret == Config::FOUND) && (set_option(hx, value) != HXHIM_SUCCESS))) {
        return false;
    }

    return true;
}

/**
 * parse_value
 * This function template is a helper function for
 * parsing and setting char * configuration values
 *
 * @param hx          the hxhim instance being built
 * @param config      the configuration
 * @param key         the key within the configuration to look for
 * @param set_option  the function to do the setting of the option
 * @param true, or false on error
 */
static bool parse_value(hxhim_t *hx, const Config::Config &config, const std::string &key, int (*set_option)(hxhim_t *, const char *)) {
    std::string value;
    const int ret = Config::get_value(config, key, value);
    if ((ret == Config::ERROR)                                                       ||
        ((ret == Config::FOUND) && (set_option(hx, value.c_str()) != HXHIM_SUCCESS))) {
        return false;
    }

    return true;
}

/**
 * parse_map_value
 * This function template is a helper function
 * for parsing and setting values that are mapped
 *
 * @param hx          the hxhim instance being built
 * @param config      the configuration
 * @param key         the key within the configuration to look for
 * @param map         the map to search the key for
 * @param set_option  the function to do the setting of the option
 * @param true, or false on error
 */
template <typename T, typename = enable_if_t <!std::is_reference<T>::value> >
bool parse_map_value(hxhim_t *hx, const Config::Config &config, const std::string &key, const std::unordered_map<std::string, T> &map, int (*set_option)(hxhim_t *, const T)) {
    T value = {};
    const int ret = Config::get_from_map(config, key, map, value);
    if ((ret == Config::ERROR)                                               ||
        ((ret == Config::FOUND) && (set_option(hx, value) != HXHIM_SUCCESS))) {
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
 * @param hx       the hxhim instance being built
 * @param config   the configuration to use
 * @return a boolean indicating whether or not an error was encountered
 */
static bool parse_datastore(hxhim_t *hx, const Config::Config &config) {
    // required
    Datastore::Type datastore;
    if ((Config::get_from_map(config,
                              hxhim::config::DATASTORE_TYPE,
                              hxhim::config::DATASTORES,
                              datastore) != Config::FOUND) ||
        (Config::get_value(config,
                           hxhim::config::DATASTORE_PREFIX,
                           hx->p->range_server.datastores.prefix) != Config::FOUND) ||
        (Config::get_value(config,
                           hxhim::config::DATASTORE_BASENAME,
                           hx->p->range_server.datastores.basename) != Config::FOUND)) {
        return false;
    }

    // optional
    Config::get_value(config,
                      hxhim::config::DATASTORE_POSTFIX,
                      hx->p->range_server.datastores.postfix);

    // datastore specific configuration
    switch (datastore) {
        case Datastore::IN_MEMORY:
            return (hxhim_set_datastore_in_memory(hx) == HXHIM_SUCCESS);

        #if HXHIM_HAVE_LEVELDB
        case Datastore::LEVELDB:
            {
                bool create_if_missing = true; // default to true; do not error if not found
                if (Config::get_value(config, hxhim::config::LEVELDB_CREATE_IF_MISSING,
                    create_if_missing) == Config::ERROR) {
                    return false;
                }

                return (hxhim_set_datastore_leveldb(hx,
                                                    create_if_missing) == HXHIM_SUCCESS);
            }
        #endif

        #if HXHIM_HAVE_ROCKSDB
        case Datastore::ROCKSDB:
            {
                bool create_if_missing = true; // default to true; do not error if not found
                if (Config::get_value(config, hxhim::config::ROCKSDB_CREATE_IF_MISSING,
                                      create_if_missing) == Config::ERROR) {
                    return false;
                }

                return (hxhim_set_datastore_rocksdb(hx,
                                                    create_if_missing) == HXHIM_SUCCESS);
            }
        #endif

        // should never get here
        default:
            return false;
    }

    // should never get here
    return false;
}

static bool parse_hash(hxhim_t *hx, const Config::Config &config) {
    return parse_value(hx, config, hxhim::config::HASH, hxhim_set_hash_name);
}

static bool parse_transport(hxhim_t *hx, const Config::Config &config) {
    Transport::Type transport_type;
    const int ret = Config::get_from_map(config, hxhim::config::TRANSPORT, hxhim::config::TRANSPORTS, transport_type);
    if (ret == Config::ERROR) {
        return false;
    }

    if (ret == Config::FOUND) {
        switch (transport_type) {
            case Transport::TRANSPORT_NULL:
                {
                    return ((hxhim_set_transport_null(hx)                   == HXHIM_SUCCESS) &&
                            (hxhim_set_hash_name(hx, "RANK_MOD_DATASTORES") == HXHIM_SUCCESS));
                }
                break;
            case Transport::TRANSPORT_MPI:
                {
                    std::size_t listeners;
                    if (Config::get_value(config, hxhim::config::MPI_LISTENERS, listeners) != Config::FOUND) {
                        return false;
                    }

                    return ((hxhim_set_transport_mpi(hx, listeners) == HXHIM_SUCCESS) &&
                           parse_hash(hx, config));
                }
                break;
            #if HXHIM_HAVE_THALLIUM
            case Transport::TRANSPORT_THALLIUM:
                {
                    Config::Config_it thallium_module = config.find(hxhim::config::THALLIUM_MODULE);
                    if (thallium_module == config.end()) {
                        return false;
                    }

                    int thread_count = -1;

                    /* thread count will default to -1, so ignore errors */
                    Config::Config_it thallium_thread_count = config.find(hxhim::config::THALLIUM_THREAD_COUNT);
                    if (thallium_thread_count != config.end()) {
                        std::stringstream(thallium_thread_count->second) >> thread_count;
                    }

                    if (thread_count < -1) {
                        thread_count = -1;
                    }

                    return ((hxhim_set_transport_thallium(hx,
                                                          thallium_module->second,
                                                          thread_count) == HXHIM_SUCCESS) &&
                            parse_hash(hx, config));
                }
                break;
            #endif
            default:
                break;
        }
    }

    return false;
}

static bool parse_endpointgroup(hxhim_t *hx, const Config::Config &config) {
    Config::Config_it endpointgroup = config.find(hxhim::config::TRANSPORT_ENDPOINT_GROUP);
    if (endpointgroup != config.end()) {
        hxhim_clear_endpoint_group(hx);
        if (endpointgroup->second == "ALL") {
            int size = -1;
            if (MPI_Comm_size(hx->p->bootstrap.comm, &size) != MPI_SUCCESS) {
                return false;
            }

            for(int rank = 0; rank < size; rank++) {
                hxhim_add_endpoint_to_group(hx, rank);
            }
            return true;
        }
        else {
            int id;
            while (std::stringstream(endpointgroup->second) >> id) {
                if (hxhim_add_endpoint_to_group(hx, id) != HXHIM_SUCCESS) {
                    // should probably write to mlog and continue instead of returning
                    return false;
                }
            }
        }
    }
    return false;
}

static bool parse_elen(hxhim_t *hx, const Config::Config &config) {
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

    return (hxhim_set_transform_numeric_values(hx, neg, pos, flt_precision, dbl_precision) == HXHIM_SUCCESS);
}

static bool parse_histogram(hxhim_t *hx, const Config::Config &config) {
    if (!parse_value(hx, config, hxhim::config::HISTOGRAM_FIRST_N,          hxhim_set_histogram_first_n)         ||
        !parse_value(hx, config, hxhim::config::HISTOGRAM_BUCKET_GEN_NAME,  hxhim_set_histogram_bucket_gen_name)) {
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

    if (hxhim_datastore_histograms(hx, read, write) != HXHIM_SUCCESS) {
        return false;
    }

    Config::Config_it hist_name_it = config.find(hxhim::config::HISTOGRAM_TRACK_PREDICATES);
    if (hist_name_it != config.end()) {
        std::stringstream s(hist_name_it->second);
        std::string name;
        while (std::getline(s, name, ',')) {
            hx->p->histograms.names.emplace(name);
        }
    }

    return true;
}

/**
 * fill_options
 * Fills up hx as best it can using config.
 * The config can be incomplete, so long as each
 * set of configuration variables is complete.
 * New values will overwrite old values.
 *
 * @param hx      the hxhim instance being built
 * @param config  the configuration to use
 * @param HXHIM_SUCCESS or HXHIM_ERROR on error
 */
static int fill_options(hxhim_t *hx, const Config::Config &config) {
    if (!hx || !hx->p ||
        (hx->p->bootstrap.comm == MPI_COMM_NULL)) {
        return HXHIM_ERROR;
    }

    using namespace hxhim::config;

    return
        parse_map_value(hx, config, DEBUG_LEVEL, DEBUG_LEVELS, hxhim_set_debug_level)                 &&
        parse_value(hx, config, CLIENT_RATIO,                  hxhim_set_client_ratio)                &&
        parse_value(hx, config, SERVER_RATIO,                  hxhim_set_server_ratio)                &&
        parse_value(hx, config, DATASTORES_PER_SERVER,         hxhim_set_datastores_per_server)       &&
        parse_datastore(hx, config)                                                                   &&
        parse_transport(hx, config)                                                                   &&
        parse_endpointgroup(hx, config)                                                               &&
        parse_value(hx, config, START_ASYNC_PUTS_AT,           hxhim_set_start_async_puts_at)         &&
        parse_value(hx, config, MAXIMUM_OPS_PER_REQUEST,       hxhim_set_maximum_ops_per_request)     &&
        parse_value(hx, config, MAXIMUM_SIZE_PER_REQUEST,      hxhim_set_maximum_size_per_request)    &&
        parse_elen(hx, config)                                                                        &&
        parse_histogram(hx, config)                                                                   &&
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
 * hx should be cleaned up by the calling function.
 *
 * @param hx         the hxhim instance being built
 * @param sequence   the configuration reader that has been set up
 * @return whether or not hx was successfully filled
 */
int process_config_and_fill_options(hxhim_t *hx, Config::Sequence &sequence) {
    if (!hx || !hx->p) {
        return HXHIM_ERROR;
    }

    // Parse the configuration data
    Config::Config config;
    sequence.process(config);

    // fill hx with values from configuration
    return fill_options(hx, config);
}

/**
 * default_reader
 * This function acts as both the default
 * configuration reader as well as an example
 * of how custom configuration readers should
 * be implmented.
 *
 * @param hx   the hxhim instance being built
 * @return HXHIM_SUCCESS, or HXHIM_ERROR if filling in the default configuration failed
 */
namespace hxhim {
namespace config {

int default_reader(hxhim_t *hx, MPI_Comm comm) {
    if (!hx || !hx->p) {
        return HXHIM_ERROR;
    }

    hxhim_set_mpi_bootstrap(hx, comm);

    // fill in the options with default values
    if ((fill_options(hx, hxhim::config::DEFAULT_CONFIG) != HXHIM_SUCCESS)) {
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

    // read the configuration and overwrite default values
    process_config_and_fill_options(hx, sequence);

    for(Config::EnvironmentVar *var : vars) {
        delete var;
    }

    return HXHIM_SUCCESS;
}

}
}
