#include <vector>
#include <sstream>

#include "hxhim/config.h"
#include "hxhim/config.hpp"
#include "hxhim/constants.h"
#include "hxhim/options.h"
#include "hxhim/options_private.hpp"
#include "utils/Configuration.hpp"
#include "utils/Histogram.hpp"

static int fill_options(hxhim_options_t *opts, const Config &config) {
    if (!opts || !opts->p ||
        (opts->p->mpi.comm == MPI_COMM_NULL)) {
        return HXHIM_ERROR;
    }

    int ret = HXHIM_SUCCESS;

    // Set the backend
    hxhim_backend_t backend;
    hxhim_backend_config_t *backend_config = nullptr;
    if (get_from_map(config, HXHIM_BACKEND_TYPE, HXHIM_BACKENDS, backend) != CONFIG_ERROR) {
        if (backend == HXHIM_BACKEND_MDHIM) {
            hxhim_mdhim_config_t *cfg = new hxhim_mdhim_config_t();
            if (!cfg) {
                return HXHIM_ERROR;
            }

            // get the location of the MDHIM config file
            Config_it mdhim_config = config.find(HXHIM_MDHIM_CONFIG);
            if (mdhim_config == config.end()) {
                return HXHIM_ERROR;
            }

            cfg->path = mdhim_config->second;
            backend_config = cfg;
        }
        else if (backend == HXHIM_BACKEND_LEVELDB) {
            hxhim_leveldb_config_t *cfg = new hxhim_leveldb_config_t();
            if (!cfg) {
                return HXHIM_ERROR;
            }

            // get the leveldb database name prefix
            Config_it name = config.find(HXHIM_LEVELDB_NAME);
            if (name == config.end()) {
                return HXHIM_ERROR;
            }
            cfg->path = name->second;

            cfg->create_if_missing = true; // default to true
            if (get_bool(config, HXHIM_LEVELDB_CREATE_IF_MISSING, cfg->create_if_missing) == CONFIG_ERROR) {
                return HXHIM_ERROR;
            }

            backend_config = cfg;
        }
        else if (backend == HXHIM_BACKEND_IN_MEMORY) {}
        else {
            return HXHIM_ERROR;
        }

        hxhim_options_set_backend(opts, backend, backend_config);
    }

    // Set Queued Bulk Puts
    std::size_t queued_bputs = 0;
    ret = get_value(config, HXHIM_QUEUED_BULK_PUTS, queued_bputs);
    if ((ret == CONFIG_ERROR) ||
        hxhim_options_set_queued_bputs(opts, queued_bputs) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    // Set Histogram Use First N Values
    std::size_t use_first_n = 0;
    ret = get_value(config, HXHIM_HISTOGRAM_FIRST_N, use_first_n);
    if ((ret == CONFIG_ERROR) ||
        hxhim_options_set_histogram_first_n(opts, use_first_n) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    // Set Histogram Bucket Generation Method
    std::string method;
    ret = get_value(config, HXHIM_HISTOGRAM_BUCKET_GEN_METHOD, method);
    if ((ret == CONFIG_ERROR) ||
        hxhim_options_set_histogram_bucket_gen_method(opts, method.c_str()) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
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
 * @param config_reader the configuration reader that has been set up
 * @param opts          the options to fill
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
    for(std::pair<const std::string, std::string> const &default_config : HXHIM_DEFAULT_CONFIG) {
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
