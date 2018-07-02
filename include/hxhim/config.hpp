#ifndef HXHIM_CONFIG_PRIVATE
#define HXHIM_CONFIG_PRIVATE

#include <map>
#include <string>

#include "utils/Configuration.hpp"
#include "constants.h"

/**
 * Constant locations where the configuration reader searches
 */
const std::string HXHIM_CONFIG_FILE               = "hxhim.conf";
const std::string HXHIM_CONFIG_ENV                = "HXHIM_CONFIG";

const std::string HXHIM_BACKEND_TYPE              = "BACKEND";                   // See HXHIM_BACKEND_TYPES
const std::string HXHIM_MDHIM_CONFIG              = "MDHIM_CONFIG";              // file path
const std::string HXHIM_LEVELDB_NAME              = "LEVELDB_NAME";              // file path
const std::string HXHIM_LEVELDB_CREATE_IF_MISSING = "LEVELDB_CREATE_IF_MISSING"; // boolean
const std::string HXHIM_QUEUED_BULK_PUTS          = "QUEUED_BULK_PUTS";          // nonnegative integer

/**
 * Set of allowed backends for HXHIM
 */
const std::map<std::string, int> HXHIM_BACKEND_TYPES = {
    std::make_pair("MDHIM",   HXHIM_BACKEND_MDHIM),
    std::make_pair("LEVELDB", HXHIM_BACKEND_LEVELDB),
};

const Config HXHIM_DEFAULT_CONFIG = {
    std::make_pair(HXHIM_BACKEND_TYPE,     "MDHIM"),
    std::make_pair(HXHIM_MDHIM_CONFIG,     "mdhim.conf"),
    std::make_pair(HXHIM_QUEUED_BULK_PUTS, "5"),
};

#endif
