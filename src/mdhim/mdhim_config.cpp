#include <cstring>
#include <cstdlib>
#include <dirent.h>
#include <fstream>
#include <sys/types.h>

#include "ConfigReader.hpp"
#include "mdhim_config.h"
#include "mdhim_constants.h"
#include "mdhim_options_private.h"
#include "transport_mpi.hpp"
#include "transport_thallium.hpp"

/**
 * parse_file
 * Generic key-value stream parser
 *
 * @param config the configuration to fill
 * @param stream the input stream
 * @return whether or not something was read
 */
static bool parse_file(ConfigReader::Config_t &config, std::istream& stream) {
    std::string key, value;
    while (stream >> key >> value) {
        config[key] = value;
    }

    return config.size();
}

/**
 * config_file
 * Try opening a configuration file
 *
 * @param config   the configuration to fill
 * @param filename the name of the file
 * @return whether or not the parse succeeded
 */
bool config_file(ConfigReader::Config_t &config, const std::string &filename) {
    std::ifstream f(filename);
    return parse_file(config, f);
}

/**
 * config_file
 * Try opening the directory where the configuration file should be
 *
 * @param config    the configuration to fill
 * @param directory the name of the directory
 * @return whether or not the parse succeeded
 */
bool config_directory(ConfigReader::Config_t &config, const std::string &directory) {
    DIR *dirp = opendir(directory.c_str());
    struct dirent *entry = nullptr;
    while ((entry = readdir(dirp))) {
        // do something with the entry
    }
    closedir(dirp);
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
bool config_environment(ConfigReader::Config_t &config, const std::string &var) {
    char *env = getenv(var.c_str());
    if (env) {
        std::ifstream f(env);
        return parse_file(config, f);
    }
    return false;
}

/**
 * fill_options
 * Fill in a mdhim_options_t with a given configuration
 *
 * This function should not be modified if the configuration
 * is not being modified.
 *
 * @param config the configuration to use
 * @param opts   the option struct to fill
 * @return whether or not opts was successfully filled
 */
static int fill_options(ConfigReader::Config_t &config, mdhim_options_t *opts) {
    if (!opts) {
        return MDHIM_ERROR;;
    }

    // initialize options
    if (mdhim_options_init(opts) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    // use the config to set options

    ConfigReader::Config_t::const_iterator use_mpi = config.find("USE_MPI");
    if ((use_mpi != config.end()) && (use_mpi->second == "true")) {
        mdhim_options_set_transport(opts, MDHIM_TRANSPORT_MPI, new MPI_Comm(MPI_COMM_WORLD));
    }

    ConfigReader::Config_t::const_iterator use_thallium = config.find("USE_THALLIUM");
    if ((use_thallium != config.end()) && (use_thallium->second == "true")) {
        mdhim_options_set_transport(opts, MDHIM_TRANSPORT_THALLIUM, new std::string(config.at("THALLIUM_MODULE")));
    }

    return MDHIM_SUCCESS;
}

/**
 * read_config_and_fill_options
 * This function is just a logical separator between
 * setting up the configuration reader and filling out
 * the mdhim_options_t
 *
 * This function should not be modified unless the
 * ConfigReader interface changes.
 *
 * @param config_reader the configuration reader that has been set up
 * @param opts          the options to fill
 * @return whether or not opts was successfully filled
 */
static int read_config_and_fill_options(ConfigReader &config_reader, mdhim_options_t *opts) {
    ConfigReader::Config_t config;
    if (!config_reader.read(config)) {
        mdhim_options_destroy(opts);
        return MDHIM_ERROR;
    }

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
    ConfigReader config_reader;

    // add default search locations in order of preference: environmental variable, file, and directory
    config_reader.add(config_environment, MDHIM_CONFIG_ENV);
    config_reader.add(config_file, MDHIM_CONFIG_FILE);
    config_reader.add(config_directory, MDHIM_CONFIG_DIR);

    return read_config_and_fill_options(config_reader, opts);
}
