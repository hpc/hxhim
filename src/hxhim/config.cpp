#include <vector>

#include "hxhim/backend/leveldb.hpp"
#include "hxhim/backend/mdhim.hpp"
#include "hxhim/config.h"
#include "hxhim/config.hpp"
#include "hxhim/constants.h"
#include "hxhim/private.hpp"
#include "mdhim/config.hpp"
#include "mdhim/mdhim.hpp"
#include "utils/Configuration.hpp"

static int fill_options(hxhim_t *hx, const Config &config) {
    if (!hx || !hx->p ||
        (hx->p->mpi.comm == MPI_COMM_NULL)) {
        return HXHIM_ERROR;
    }

    std::size_t watermark = 0;
    switch (get_value(config, HXHIM_QUEUED_BULK_PUTS, watermark)) {
        case CONFIG_FOUND:
            hx->p->watermark = watermark;
            break;
        case CONFIG_ERROR:
        default:
            return HXHIM_ERROR;
    }

    if (get_from_map(config, HXHIM_SUBJECT_TYPE, HXHIM_SPO_TYPES, hx->p->subject_type) == CONFIG_ERROR) {
        return HXHIM_ERROR;
    }

    if (get_from_map(config, HXHIM_PREDICATE_TYPE, HXHIM_SPO_TYPES, hx->p->predicate_type) == CONFIG_ERROR) {
        return HXHIM_ERROR;
    }

    if (get_from_map(config, HXHIM_OBJECT_TYPE, HXHIM_SPO_TYPES, hx->p->object_type) == CONFIG_ERROR) {
        return HXHIM_ERROR;
    }

    int backend;
    if (get_from_map(config, HXHIM_BACKEND_TYPE, HXHIM_BACKEND_TYPES, backend) == CONFIG_FOUND) {
        if (backend == HXHIM_BACKEND_MDHIM) {
            // read the MDHIM configuration
            Config_it mdhim_config = config.find(HXHIM_MDHIM_CONFIG);
            if (mdhim_config == config.end()) {
                return HXHIM_ERROR;
            }

            hx->p->backend = new hxhim::backend::mdhim(hx->p->mpi.comm, mdhim_config->second);
        }
        else if (backend == HXHIM_BACKEND_LEVELDB) {
            Config_it name = config.find(HXHIM_LEVELDB_NAME);
            if (name == config.end()) {
                return HXHIM_ERROR;
            }

            bool create_if_missing = true; // default to true
            if (get_bool(config, HXHIM_LEVELDB_CREATE_IF_MISSING, create_if_missing) == CONFIG_ERROR) {
                return HXHIM_ERROR;
            }

            hx->p->backend = new hxhim::backend::leveldb(name->second, hx->p->mpi.comm, hx->p->mpi.rank, create_if_missing);
        }
        else {
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
int process_config_and_fill_options(hxhim_t *hx, ConfigSequence &config_sequence) {
    if (!hx || !hx->p) {
        return HXHIM_ERROR;
    }

    // Parse the configuration data
    Config config;
    config_sequence.process(config);

    // fill opts with values from configuration
    return fill_options(hx, config);
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
int hxhim_default_config_reader(hxhim_t *hx) {
    if (!hx || !hx->p) {
        return HXHIM_ERROR;
    }

    ConfigSequence config_sequence;

    // add default search locations in order of preference: default filename, file environment variable, environment variable overrides
    ConfigFile file(HXHIM_CONFIG_FILE);
    config_sequence.add(&file);

    ConfigFileEnvironment fileenv(HXHIM_CONFIG_ENV);
    config_sequence.add(&fileenv);

    std::vector<ConfigVarEnvironment *> vars;
    for(std::pair<const std::string, std::string> const &default_config : HXHIM_DEFAULT_CONFIG) {
        ConfigVarEnvironment *var = new ConfigVarEnvironment(default_config.first);
        config_sequence.add(var);
        vars.push_back(var);
    }

    int ret = HXHIM_SUCCESS;
    if (process_config_and_fill_options(hx, config_sequence) != HXHIM_SUCCESS) { // read the configuration and overwrite default values
        ret = HXHIM_ERROR;
    }

    for(ConfigVarEnvironment *&var : vars) {
        delete var;
    }

    return ret;
}
