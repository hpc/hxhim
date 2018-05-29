#include "mdhim_config.hpp"
#include "hxhim_config.hpp"
#include "hxhim_private.hpp"

static int fill_options(hxhim_t *hx, const Config &config, MPI_Comm comm) {
    if (!hx || !hx->p ||
        (comm == MPI_COMM_NULL)) {
        return HXHIM_ERROR;
    }

    // read the MDHIM configuration
    Config_it mdhim_config = config.find(MDHIM_CONFIG);
    if (mdhim_config != config.end()) {
        ConfigSequence config_sequence;

        ConfigFile file(mdhim_config->second);
        config_sequence.add(&file);

        // fill in the MDHIM options
        if (!(hx->p->mdhim_opts = new mdhim_options_t())                                           ||
            (mdhim_options_init(hx->p->mdhim_opts, comm, false, false)           != MDHIM_SUCCESS) ||
            (process_config_and_fill_options(config_sequence, hx->p->mdhim_opts) != MDHIM_SUCCESS)) {
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
    if (!config_sequence.process(config)) {
        return HXHIM_ERROR;
    }

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

    // add default search locations in order of preference: environmental variable, file, and directory
    ConfigEnvironment env(HXHIM_CONFIG_ENV);
    config_sequence.add(&env);

    ConfigFile file(HXHIM_CONFIG_FILE);
    config_sequence.add(&file);

    if (process_config_and_fill_options(hx, config_sequence, comm) != HXHIM_SUCCESS) { // read the configuration and overwrite default values
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}
