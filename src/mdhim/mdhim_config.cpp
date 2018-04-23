#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <fstream>
#include <iomanip>
#include <sys/types.h>

#include "mdhim_config.h"
#include "mdhim_config.hpp"
#include "mdhim_constants.h"
#include "mdhim_options_private.h"
#include "transport_mpi.hpp"
#include "transport_thallium.hpp"

#define MDHIM_CONFIG_NOT_FOUND (MDHIM_DB_ERROR - 1)
using Config_it =  Config::const_iterator;

/**
 * parse_file
 * Generic key-value stream parser
 *
 * @param config the configuration to fill
 * @param stream the input stream
 * @return whether or not something was read
 */
static bool parse_file(Config &config, std::istream& stream) {
    std::string key, value;
    while (stream >> key >> value) {
        config[key] = value;
    }

    return config.size();
}

ConfigFile::ConfigFile(const std::string &filename)
  : ConfigReader(),
    filename_(filename)
{}

ConfigFile::~ConfigFile() {}

bool ConfigFile::process(Config &config) const {
    std::ifstream f(filename_);
    return parse_file(config, f);
}

ConfigDirectory::ConfigDirectory(const std::string &directory)
  : ConfigReader(),
    directory_(directory)
{}

ConfigDirectory::~ConfigDirectory() {}

bool ConfigDirectory::process(Config &config) const {
    DIR *dirp = opendir(directory_.c_str());
    struct dirent *entry = nullptr;
    while ((entry = readdir(dirp))) {
        // do something with the entry
    }
    closedir(dirp);
    return false;

}

ConfigEnvironment::ConfigEnvironment(const std::string& variable)
  : ConfigReader(),
    variable_(variable)
{}

ConfigEnvironment::~ConfigEnvironment() {}

bool ConfigEnvironment::process(Config &config) const {
    char *env = getenv(variable_.c_str());
    if (env) {
        std::ifstream f(env);
        return parse_file(config, f);
    }
    return false;
}


/**
 * config_environment
 * Try opening a configuration file specified by an environment variable
 *
 * @param config  the configuration to fill
 * @param var     the environment variable to look up
 * @return whether or not the parse succeeded
 */
bool config_environment(Config &config, const std::string &var) {
    char *env = getenv(var.c_str());
    if (env) {
        std::ifstream f(env);
        return parse_file(config, f);
    }
    return false;
}

/**
 * get_bool
 * Helper function for reading booleans from the configuration
 *
 * @param config     the configuration
 * @param config_key the entry in the configuraion to read
 * @param b          the value of the configuration
 * @return MDHIM_SUCCESS if the configuration value was good, MDHIM_CONFIG_NOT_FOUND if the configuration key was not found, or MDHIM_ERROR if the configuration value was bad
 */
int get_bool(const Config &config, const std::string &config_key, bool &b) {
    // find the key
    Config_it in_config = config.find(config_key);
    if (in_config != config.end()) {
        std::string config_value = in_config->second;
        std::transform(config_value.begin(), config_value.end(), config_value.begin(), ::tolower);
        return (std::stringstream(config_value) >> std::boolalpha >> b)?MDHIM_SUCCESS:MDHIM_ERROR;
    }

    return MDHIM_CONFIG_NOT_FOUND;
}

/**
 * get_integral
 * Helper function for reading integral values from the configuration
 *
 * @param config     the configuration
 * @param config_key the entry in the configuraion to read
 * @tparam i         the value of the configuration
 * @return MDHIM_SUCCESS if the configuration value was good, MDHIM_CONFIG_NOT_FOUND if the configuration key was not found, or MDHIM_ERROR if the configuration value was bad
 */
template<typename T, typename = std::enable_if_t<std::is_integral<T>::value> >
int get_integral(const Config &config, const std::string &config_key, T &i) {
    // find the key
    Config_it in_config = config.find(config_key);
    if (in_config != config.end()) {
        return (std::stringstream(in_config->second) >> i)?MDHIM_SUCCESS:MDHIM_ERROR;
    }

    return MDHIM_CONFIG_NOT_FOUND;
}

/**
 * get_from_map
 * Helper function for getting values from a map with the value read from the configuration
 *
 * @param config     the configuration
 * @param config_key the entry in the configuraion to read
 * @tparam map       the map where the config_key should be found
 * @tparam value     the value of the configuration
 * @return MDHIM_SUCCESS if the configuration value was good, MDHIM_CONFIG_NOT_FOUND if the configuration key was not found, or MDHIM_ERROR if the configuration value was bad
 */
template<typename T>
int get_from_map(const Config &config, const std::string &config_key,
                 const std::map<std::string, T> &map, T &value) {
    // find key in configuration
    Config_it in_config = config.find(config_key);
    if (in_config!= config.end()) {
        // use value to get internal value from map
        typename std::map<std::string, T>::const_iterator in_map = map.find(in_config->second);
        if (in_map == map.end()) {
            return MDHIM_ERROR;
        }

        value = in_map->second;
        return MDHIM_SUCCESS;
    }

    return MDHIM_CONFIG_NOT_FOUND;
}

/**
 * fill_options
 * Fill in a mdhim_options_t with a given configuration
 *
 * This function should not be modified if the configuration
 * is not being modified.
 *
 * opts is not cleaned up here. It should be cleaned up by
 * the top-level calling function.
 *
 * @param config the configuration to use
 * @param opts   the option struct to fill
 * @return whether or not opts was successfully filled
 */
static int fill_options(const Config &config, mdhim_options_t *opts) {
    // no need to check for null opts

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
    if ((ret != MDHIM_SUCCESS)                                                                  ||
        ((ret == MDHIM_SUCCESS) && (mdhim_options_set_db_type(opts, db_type) != MDHIM_SUCCESS))) {
        return MDHIM_ERROR;
    }

    // Set Server Factor
    int server_factor;
    ret = get_integral(config, SERVER_FACTOR, server_factor);
    if ((ret == MDHIM_ERROR)                                                                                ||
        ((ret == MDHIM_SUCCESS) && (mdhim_options_set_server_factor(opts, server_factor) != MDHIM_SUCCESS))) {
        return MDHIM_ERROR;
    }

    // Set Maximum Number of Records Per Slice
    int max_recs_per_slice;
    ret = get_integral(config, MAX_RECS_PER_SLICE, max_recs_per_slice);
    if ((ret == MDHIM_ERROR)                                                                                          ||
        ((ret == MDHIM_SUCCESS) && (mdhim_options_set_max_recs_per_slice(opts, max_recs_per_slice) != MDHIM_SUCCESS))) {
        return MDHIM_ERROR;
    }

    // Set Key Type
    int key_type;
    ret = get_from_map(config, KEY_TYPE, KEY_TYPES, key_type);
    if ((ret != MDHIM_SUCCESS)                                                                    ||
        ((ret == MDHIM_SUCCESS) && (mdhim_options_set_key_type(opts, key_type) != MDHIM_SUCCESS))) {
        return MDHIM_ERROR;
    }

    // Set Debug Level
    int debug_level;
    ret = get_from_map(config, DEBUG_LEVEL, DEBUG_LEVELS, debug_level);
    if ((ret != MDHIM_SUCCESS)                                                                          ||
        ((ret == MDHIM_SUCCESS) && (mdhim_options_set_debug_level(opts, debug_level) != MDHIM_SUCCESS))) {
        return MDHIM_ERROR;
    }

    // Set Number of Worker Threads
    int num_worker_threads;
    ret = get_integral(config, NUM_WORKER_THREADS, num_worker_threads);
    if ((ret == MDHIM_ERROR)                                                                                          ||
        ((ret == MDHIM_SUCCESS) && (mdhim_options_set_num_worker_threads(opts, num_worker_threads) != MDHIM_SUCCESS))) {
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
    if ((ret == MDHIM_ERROR)                                                                                ||
        ((ret == MDHIM_SUCCESS) && (mdhim_options_set_create_new_db(opts, create_new_db) != MDHIM_SUCCESS))) {
        return MDHIM_ERROR;
    }

    // Set DB Write Mode
    int db_write;
    ret = get_from_map(config, DB_WRITE, DB_WRITES, db_write);
    if ((ret != MDHIM_SUCCESS)                                                                        ||
        ((ret == MDHIM_SUCCESS) && (mdhim_options_set_value_append(opts, db_write) != MDHIM_SUCCESS))) {
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
    if ((get_bool(config, USE_MPI, use_mpi) == MDHIM_SUCCESS) &&
        use_mpi) {
        int memory_alloc_size, memory_regions;
        if ((get_integral(config, MEMORY_ALLOC_SIZE, memory_alloc_size) != MDHIM_SUCCESS) ||
            (get_integral(config, MEMORY_REGIONS, memory_regions)       != MDHIM_SUCCESS)) {
            return MDHIM_ERROR;
        }

        if (mdhim_options_init_mpi_transport(opts, MPI_COMM_WORLD, memory_alloc_size, memory_regions) != MDHIM_SUCCESS) {
            return MDHIM_ERROR;
        }
    }
    // prefer MPI
    else {
        // Use thallium as the transport
        bool use_thallium;
        if ((get_bool(config, USE_THALLIUM, use_thallium) == MDHIM_SUCCESS) &&
            use_thallium) {
            if (mdhim_options_init_thallium_transport(opts, config.at(THALLIUM_MODULE).c_str()) != MDHIM_SUCCESS) {
                return MDHIM_ERROR;
            }
        }
    }

    return MDHIM_SUCCESS;
}

/**
 * read_config_and_fill_options
 * This function is just a logical separator between
 * setting up the configuration reader and filling out
 * the mdhim_options_t.
 *
 * This function should be common for all configuration readers.
 *
 * This function should not be modified unless the
 * ConfigReader interface changes.
 *
 * opts should be cleand up by the calling function.
 *
 * @param config_reader the configuration reader that has been set up
 * @param opts          the options to fill
 * @return whether or not opts was successfully filled
 */
int process_config_and_fill_options(ConfigSequence &config_sequence, mdhim_options_t *opts) {
    // Parse the configuration data
    Config config;
    if (!config_sequence.process(config)) {
        return MDHIM_ERROR;
    }

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
 * @return whether or not configuration was completed
 */
int mdhim_default_config_reader(mdhim_options_t *opts) {
    ConfigSequence config_sequence;

    // add default search locations in order of preference: environmental variable, file, and directory
    ConfigEnvironment env(MDHIM_CONFIG_ENV);
    config_sequence.add(&env);

    ConfigFile file(MDHIM_CONFIG_FILE);
    config_sequence.add(&file);

    ConfigDirectory dir(MDHIM_CONFIG_DIR);
    config_sequence.add(&dir);

    if ((mdhim_options_init(opts)                               != MDHIM_SUCCESS) || // initialize opts->p
        (mdhim_options_init_db(opts, false)                     != MDHIM_SUCCESS) || // initialize opts->p->db
        (fill_options(MDHIM_DEFAULT_CONFIG, opts)               != MDHIM_SUCCESS) || // fill in the configuration with default values
        (process_config_and_fill_options(config_sequence, opts) != MDHIM_SUCCESS)) { // read the configuration and overwrite default values
        mdhim_options_destroy(opts);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}
