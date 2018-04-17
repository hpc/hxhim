#ifndef MDHIM_CONFIG_PRIVATE
#define MDHIM_CONFIG_PRIVATE

#include "ConfigReader.hpp"
#include "mdhim_options.h"

/**
 * These some example functions for parsing key value pairs.
 * They are used in the default configuration reader.
 */
bool config_file(ConfigReader::Config_t &config, const std::string &filename);
bool config_directory(ConfigReader::Config_t &config, const std::string &directory);
bool config_environment(ConfigReader::Config_t &config, const std::string &var);

/**
 * This function should only be called
 * by a custom configuration reader.
 *
 * It should be static, but needs to
 * be accesible to custom configuration
 * readers implemented in other files.
 */
int read_config_and_fill_options(ConfigReader &config_reader, mdhim_options_t *opts);

#endif
