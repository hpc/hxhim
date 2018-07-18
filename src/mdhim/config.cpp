#include <algorithm>
#include <sstream>
#include <type_traits>

#include "mdhim/config.h"
#include "mdhim/config.hpp"
#include "mdhim/constants.h"
#include "mdhim/options_private.h"
#include "transport/MPI.hpp"
#include "transport/Thallium.hpp"

/**
 * fill_options
 * Fill in a mdhim_options_t with a given configuration.
 * This function should only overwrite values in opts,
 * not reset opts before setting values.
 *
 * This function should not be modified if the configuration
 * is not being modified.
 *
 * opts should neither be allocated or deallocated here.
 * When opts is passed in, its main pointers should be
 * ready for filling in. If there is an error, this function
 * simply returns MDHIM_ERROR. opts should be cleaned up by
 * the top-level calling function.
 *
 * @param config the configuration to use
 * @param opts   the option struct to fill
 * @return whether or not opts was successfully filled
 */
static int fill_options(const Config &config, mdhim_options_t *opts) {
    if (!opts || !opts->p || !opts->p->transport || !opts->p->db) {
        return MDHIM_ERROR;
    }

    int ret = MDHIM_SUCCESS;

    // Set DB Path
    Config_it db_path = config.find(DB_PATH);
    if (db_path != config.end()) {
        if (mdhim_options_set_db_path(opts, db_path->second.c_str()) != MDHIM_SUCCESS) {
            return MDHIM_ERROR;
        }
    }

    // Set DB name
    Config_it db_name = config.find(DB_NAME);
    if (db_name != config.end()) {
        if (mdhim_options_set_db_name(opts, db_name->second.c_str()) != MDHIM_SUCCESS) {
            return MDHIM_ERROR;
        }
    }

    // Set DB Type
    int db_type;
    ret = get_from_map(config, DB_TYPE, DB_TYPES, db_type);
    if ((ret == CONFIG_ERROR)                                                                  ||
        ((ret == CONFIG_FOUND) && (mdhim_options_set_db_type(opts, db_type) != MDHIM_SUCCESS))) {
        return MDHIM_ERROR;
    }

    // Set Server Factor
    int rserver_factor;
    ret = get_value(config, RSERVER_FACTOR, rserver_factor);
    if ((ret == CONFIG_ERROR)                                                                               ||
        ((ret == CONFIG_FOUND) && (mdhim_options_set_server_factor(opts, rserver_factor) != MDHIM_SUCCESS))) {
        return MDHIM_ERROR;
    }

    // Set Number of databases per server
    int dbs_per_rserver;
    ret = get_value(config, DBS_PER_RSERVER, dbs_per_rserver);
    if ((ret == CONFIG_ERROR)                                                                                 ||
        ((ret == CONFIG_FOUND) && (mdhim_options_set_dbs_per_server(opts, dbs_per_rserver) != MDHIM_SUCCESS))) {
        return MDHIM_ERROR;
    }

    // Set Maximum Number of Records Per Slice
    int max_recs_per_slice;
    ret = get_value(config, MAX_RECS_PER_SLICE, max_recs_per_slice);
    if ((ret == CONFIG_ERROR)                                                                                        ||
        ((ret == CONFIG_FOUND) && (mdhim_options_set_max_recs_per_slice(opts, max_recs_per_slice) != MDHIM_SUCCESS))) {
        return MDHIM_ERROR;
    }

    // Set Key Type
    int key_type;
    ret = get_from_map(config, KEY_TYPE, KEY_TYPES, key_type);
    if ((ret == CONFIG_ERROR)                                                                    ||
        ((ret == CONFIG_FOUND) && (mdhim_options_set_key_type(opts, key_type) != MDHIM_SUCCESS))) {
        return MDHIM_ERROR;
    }

    // Set Debug Level
    int debug_level;
    ret = get_from_map(config, DEBUG_LEVEL, DEBUG_LEVELS, debug_level);
    if ((ret == CONFIG_ERROR)                                                                          ||
        ((ret == CONFIG_FOUND) && (mdhim_options_set_debug_level(opts, debug_level) != MDHIM_SUCCESS))) {
        return MDHIM_ERROR;
    }

    // Set Number of Worker Threads
    int num_worker_threads;
    ret = get_value(config, NUM_WORKER_THREADS, num_worker_threads);
    if ((ret == CONFIG_ERROR)                                                                                        ||
        ((ret == CONFIG_FOUND) && (mdhim_options_set_num_worker_threads(opts, num_worker_threads) != MDHIM_SUCCESS))) {
        return MDHIM_ERROR;
    }

    // Set Manifest Path
    Config_it manifest_path = config.find(MANIFEST_PATH);
    if (manifest_path != config.end()) {
        if (mdhim_options_set_manifest_path(opts, manifest_path->second.c_str()) != MDHIM_SUCCESS) {
            return MDHIM_ERROR;
        }
    }

    // Set Create New DB
    bool create_new_db;
    ret = get_bool(config, CREATE_NEW_DB, create_new_db);
    if ((ret == CONFIG_ERROR)                                                                              ||
        ((ret == CONFIG_FOUND) && (mdhim_options_set_create_new_db(opts, create_new_db) != MDHIM_SUCCESS))) {
        return MDHIM_ERROR;
    }

    // Set DB Write Mode
    int db_write;
    ret = get_from_map(config, DB_WRITE, DB_WRITES, db_write);
    if ((ret == CONFIG_ERROR)                                                                        ||
        ((ret == CONFIG_FOUND) && (mdhim_options_set_value_append(opts, db_write) != MDHIM_SUCCESS))) {
        return MDHIM_ERROR;
    }

    // Set DB Host
    Config_it db_host = config.find(DB_HOST);
    if (db_host != config.end()) {
        if (mdhim_options_set_db_host(opts, db_host->second.c_str()) != MDHIM_SUCCESS) {
            return MDHIM_ERROR;
        }
    }

    // Set DB Login
    Config_it db_login = config.find(DB_LOGIN);
    if (db_login != config.end()) {
        if (mdhim_options_set_db_login(opts, db_login->second.c_str()) != MDHIM_SUCCESS) {
            return MDHIM_ERROR;
        }
    }

    // Set DB Password
    Config_it db_password = config.find(DB_PASSWORD);
    if (db_password != config.end()) {
        if (mdhim_options_set_db_password(opts, db_password->second.c_str()) != MDHIM_SUCCESS) {
            return MDHIM_ERROR;
        }
    }

    // Set DBS Host
    Config_it dbs_host = config.find(DBS_HOST);
    if (dbs_host != config.end()) {
        if (mdhim_options_set_dbs_host(opts, dbs_host->second.c_str()) != MDHIM_SUCCESS) {
            return MDHIM_ERROR;
        }
    }

    // Set DBS Login
    Config_it dbs_login = config.find(DBS_LOGIN);
    if (dbs_login != config.end()) {
        if (mdhim_options_set_dbs_login(opts, dbs_login->second.c_str()) != MDHIM_SUCCESS) {
            return MDHIM_ERROR;
        }
    }

    // Set DBS Password
    Config_it dbs_password = config.find(DBS_PASSWORD);
    if (dbs_password != config.end()) {
        if (mdhim_options_set_dbs_password(opts, dbs_password->second.c_str()) != MDHIM_SUCCESS) {
            return MDHIM_ERROR;
        }
    }

    // Use MPI as the transport
    bool use_mpi;
    if ((get_bool(config, USE_MPI, use_mpi) == CONFIG_FOUND) &&
        use_mpi) {
        std::size_t memory_alloc_size, memory_regions, listeners;
        if ((get_value(config, MEMORY_ALLOC_SIZE, memory_alloc_size) != MDHIM_SUCCESS) ||
            (get_value(config, MEMORY_REGIONS, memory_regions)       != MDHIM_SUCCESS) ||
            (get_value(config, LISTENERS, listeners)                 != MDHIM_SUCCESS)) {
            return MDHIM_ERROR;
        }

        if (mdhim_options_set_mpi(opts, MPI_COMM_WORLD, memory_alloc_size, memory_regions, listeners) != MDHIM_SUCCESS) {
            return MDHIM_ERROR;
        }
    }
    // prefer MPI
    else {
        // Use thallium as the transport
        bool use_thallium;
        if ((get_bool(config, USE_THALLIUM, use_thallium) == CONFIG_FOUND) &&
            use_thallium) {

            Config_it thallium_module = config.find(THALLIUM_MODULE);
            if ((thallium_module == config.end())                                                    ||
                (mdhim_options_set_thallium(opts, thallium_module->second.c_str()) != MDHIM_SUCCESS)) {
                return MDHIM_ERROR;
            }
        }
    }

    // Add ranks to the endpoint group
    Config_it endpointgroup = config.find(ENDPOINT_GROUP);
    if (endpointgroup != config.end()) {
        mdhim_options_clear_endpoint_group(opts);
        if (endpointgroup->second == "ALL") {
            for(int rank = 0; rank < opts->size; rank++) {
                mdhim_options_add_endpoint_to_group(opts, rank);
            }
        }
        else {
            std::stringstream s(endpointgroup->second);
            int rank;
            while (s >> rank) {
                if (mdhim_options_add_endpoint_to_group(opts, rank) != MDHIM_SUCCESS) {
                    // should probably write to mlog and continue instead of returning
                    return MDHIM_ERROR;
                }
            }
        }
    }

    return MDHIM_SUCCESS;
}

/**
 * process_config_and_fill_options
 * This function is just a logical separator between
 * setting up the configuration reader and filling out
 * the mdhim_options_t.
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
int process_config_and_fill_options(ConfigSequence &config_sequence, mdhim_options_t *opts) {
    // Parse the configuration data
    Config config;
    config_sequence.process(config);

    // fill opts with values from configuration
    return fill_options(config, opts);
}

/**
 * mdhim_default_config_reader
 * This function acts as both the default
 * configuration reader as well as an example
 * of how custom configuration readers should
 * be implmented.
 *
 * @param opts the options to fill
 * @param comm the bootstrapping communicator
 * @return whether or not configuration was completed
 */
int mdhim_default_config_reader(mdhim_options_t *opts, const MPI_Comm comm, const std::string &config_filename) {
    ConfigSequence config_sequence;

    // add default search locations in order of preference: filename, file environment variable, environment variable overrides
    ConfigFile file(config_filename);
    config_sequence.add(&file);

    ConfigFileEnvironment fileenv(MDHIM_CONFIG_ENV);
    config_sequence.add(&fileenv);

    std::vector<ConfigVarEnvironment *> vars;
    for(std::pair<const std::string, std::string> const &default_config : MDHIM_DEFAULT_CONFIG) {
        ConfigVarEnvironment *var = new ConfigVarEnvironment(default_config.first);
        config_sequence.add(var);
        vars.push_back(var);
    }

    int ret = MDHIM_SUCCESS;
    if ((mdhim_options_init(opts, comm, false, false)           != MDHIM_SUCCESS) || // initialize opts->p, opts->p->transport, and opts->p->db
        (fill_options(MDHIM_DEFAULT_CONFIG, opts)               != MDHIM_SUCCESS) || // fill in the configuration with default values
        (process_config_and_fill_options(config_sequence, opts) != MDHIM_SUCCESS)) { // read the configuration and overwrite default values
        mdhim_options_destroy(opts);
        ret = MDHIM_ERROR;
    }

    for(ConfigVarEnvironment *&var : vars) {
        delete var;
    }

    return ret;
}

/**
 * mdhim_default_config_reader
 * This is the C version of the function, which does not
 * allow for the configuration file name to be set.
 *
 * @param opts the options to fill
 * @param comm the bootstrapping communicator
 * @return whether or not configuration was completed
 */
int mdhim_default_config_reader(mdhim_options_t *opts, const MPI_Comm comm) {
    return mdhim_default_config_reader(opts, comm, MDHIM_CONFIG_FILE);
}
