#ifndef HXHIM_CONFIG_PRIVATE
#define HXHIM_CONFIG_PRIVATE

#include <map>
#include <string>

#include "hxhim-types.h"

/**
 * Constant locations where the configuration reader searches
 */
const std::string HXHIM_CONFIG_FILE      = "hxhim.conf";
const std::string HXHIM_CONFIG_ENV       = "HXHIM_CONFIG";

const std::string HXHIM_MDHIM_CONFIG     = "MDHIM_CONFIG";     // file path
const std::string HXHIM_QUEUED_BULK_PUTS = "QUEUED_BULK_PUTS"; // nonnegative integer

#endif
