#ifndef MDHIM_CONFIG_PRIVATE
#define MDHIM_CONFIG_PRIVATE

#include "ConfigReader.hpp"
#include "mdhim_options.h"

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
