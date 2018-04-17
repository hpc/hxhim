#ifndef MDHIM_CONFIG_PRIVATE
#define MDHIM_CONFIG_PRIVATE

#include "ConfigReader.hpp"
#include "mdhim_options.h"

/**
 * Constant locations where the configuration reader searches
 */
#define MDHIM_CONFIG_FILE "mdhim.conf"
#define MDHIM_CONFIG_DIR  "./"
#define MDHIM_CONFIG_ENV  "MDHIM_CONFIG"

/**
 * Constants used in the mdhim configuration file
 * Assume all values are necessary even if they are not used
 */
const std::string USE_MPI           = "USE_MPI";               // true/false
const std::string MEMORY_ALLOC_SIZE = "MEMORY_ALLOC_SIZE";     // positive integer
const std::string MEMORY_REGIONS    = "MEMORY_REGIONS";        // positive integer
const std::string USE_THALLIUM      = "USE_THALLIUM";          // true/false
const std::string THALLIUM_MODULE   = "THALLIUM_MODULE";       // string

/**
 * These some example functions for parsing key value pairs.
 * They are used in the default configuration reader.
 */
bool config_file(ConfigReader::Config_t &config, const std::string &filename);
bool config_directory(ConfigReader::Config_t &config, const std::string &directory);
bool config_environment(ConfigReader::Config_t &config, const std::string &var);

/**
 * This function should only be called
 * at the end of a custom configuration reader.
 */
int read_config_and_fill_options(ConfigReader &config_reader, mdhim_options_t *opts);

#endif
