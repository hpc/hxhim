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
const std::string HXHIM_SUBJECT_TYPE              = "SUBJECT_TYPE";              // See HXHIM_SPO_TYPES
const std::string HXHIM_PREDICATE_TYPE            = "PREDICATE_TYPE";            // See HXHIM_SPO_TYPES
const std::string HXHIM_OBJECT_TYPE               = "OBJECT_TYPE";               // See HXHIM_SPO_TYPES

/**
 * Set of allowed backends for HXHIM
 */
const std::map<std::string, int> HXHIM_BACKEND_TYPES = {
    std::make_pair("MDHIM",   HXHIM_BACKEND_MDHIM),
    std::make_pair("LEVELDB", HXHIM_BACKEND_LEVELDB),
};

/**
 * The allowable subject, predicate, and object types
 */
const std::map<std::string, int> HXHIM_SPO_TYPES = {
    std::make_pair("INT",    HXHIM_INT_TYPE),
    std::make_pair("FLOAT",  HXHIM_FLOAT_TYPE),
    std::make_pair("DOUBLE", HXHIM_DOUBLE_TYPE),
    std::make_pair("BYTE",   HXHIM_BYTE_TYPE),
};

const Config HXHIM_DEFAULT_CONFIG = {
    std::make_pair(HXHIM_BACKEND_TYPE,     "MDHIM"),
    std::make_pair(HXHIM_MDHIM_CONFIG,     "mdhim.conf"),
    std::make_pair(HXHIM_QUEUED_BULK_PUTS, "5"),
    std::make_pair(HXHIM_SUBJECT_TYPE,     "BYTE"),
    std::make_pair(HXHIM_PREDICATE_TYPE,   "BYTE"),
    std::make_pair(HXHIM_OBJECT_TYPE,      "BYTE"),
};

#endif
