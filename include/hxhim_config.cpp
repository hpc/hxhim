#include <algorithm>
#include <sstream>

#include "Configuration.hpp"
#include "mdhim_config.hpp"
#include "hxhim_config.hpp"
#include "hxhim_private.hpp"
#include <iostream>

#define HXHIM_CONFIG_NOT_FOUND (HXHIM_ERROR - 1)

/**
 * get_value
 * Helper function for reading numeric values from the configuration
 *
 * @param config     the configuration
 * @param config_key the entry in the configuraion to read
 * @tparam value     the value of the configuration
 * @return HXHIM_SUCCESS if the configuration value was good, HXHIM_CONFIG_NOT_FOUND if the configuration key was not found, or HXHIM_ERROR if the configuration value was bad
 */
template<typename T, typename = std::enable_if <std::is_arithmetic<T>::value> >
static int get_value(const Config &config, const std::string &config_key, T &v) {
    // find the key
    Config_it in_config = config.find(config_key);
    if (in_config != config.end()) {
        return (std::stringstream(in_config->second) >> v)?HXHIM_SUCCESS:HXHIM_ERROR;
    }

    return HXHIM_CONFIG_NOT_FOUND;
}

static int fill_options(hxhim_t *hx, const Config &config, MPI_Comm comm) {
    if (!hx || !hx->p ||
        (comm == MPI_COMM_NULL)) {
        return HXHIM_ERROR;
    }

    int ret = HXHIM_SUCCESS;

    // read the MDHIM configuration
    Config_it mdhim_config = config.find(HXHIM_MDHIM_CONFIG);
    if (mdhim_config != config.end()) {
        ConfigSequence config_sequence;

        // fill in the MDHIM options
        if (!(hx->p->mdhim_opts = new mdhim_options_t())                                       ||
            mdhim_default_config_reader(hx->p->mdhim_opts, comm, mdhim_config->second.c_str())) {
            mdhim_options_destroy(hx->p->mdhim_opts);
            delete hx->p->mdhim_opts;
            return HXHIM_ERROR;
        }

        // fill in the mdhim_t structure
        if (!(hx->p->md = new mdhim_t())                                 ||
            (mdhim::Init(hx->p->md, hx->p->mdhim_opts) != MDHIM_SUCCESS)) {
            mdhim::Close(hx->p->md);
            delete hx->p->md;
            mdhim_options_destroy(hx->p->mdhim_opts);
            delete hx->p->mdhim_opts;
            return HXHIM_ERROR;
        }
    }

    std::size_t watermark = 0;
    ret = get_value(config, HXHIM_QUEUED_BULK_PUTS, watermark);
    if (ret == HXHIM_ERROR) {
        mdhim::Close(hx->p->md);
        delete hx->p->md;
        mdhim_options_destroy(hx->p->mdhim_opts);
        delete hx->p->mdhim_opts;
        return HXHIM_ERROR;
    }
    else if (ret == HXHIM_SUCCESS) {
        hx->p->watermark = watermark;
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
int process_config_and_fill_options(hxhim_t *hx, ConfigSequence &config_sequence, MPI_Comm comm) {
    if (!hx || !hx->p) {
        return HXHIM_ERROR;
    }

    // Parse the configuration data
    Config config;
    config_sequence.process(config);

    // fill opts with values from configuration
    return fill_options(hx, config, comm);
}

/**
 * hxhim_default_config_reader
 * This function acts as both the default
 * configuration reader as well as an example
 * of how custom configuration readers should
 * be implmented.
 *
 * @param opts the options to fill
 * @param comm the bootstrapping communicator
 * @return whether or not configuration was completed
 */
int hxhim_default_config_reader(hxhim_t *hx, const MPI_Comm comm) {
    if (!hx || !hx->p) {
        return HXHIM_ERROR;
    }

    ConfigSequence config_sequence;

    // add default search locations in order of preference: default filename, file environment variable, environment variable overrides
    ConfigFile file(HXHIM_CONFIG_FILE);
    config_sequence.add(&file);

    ConfigFileEnvironment fileenv(HXHIM_CONFIG_ENV);
    config_sequence.add(&fileenv);

    ConfigVarEnvironment mdhim_config(HXHIM_MDHIM_CONFIG);
    config_sequence.add(&mdhim_config);

    if (process_config_and_fill_options(hx, config_sequence, comm) != HXHIM_SUCCESS) { // read the configuration and overwrite default values
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}
