#ifndef HXHIM_CONFIG_PRIVATE
#define HXHIM_CONFIG_PRIVATE

#include <map>
#include <string>

#include "constants.h"
#include "utils/Configuration.hpp"
#include "utils/Histogram.hpp"

/**
 * Constant locations where the configuration reader searches
 */
const std::string HXHIM_CONFIG_FILE                 = "hxhim.conf";
const std::string HXHIM_CONFIG_ENV                  = "HXHIM_CONFIG";

const std::string HXHIM_BACKEND_TYPE                = "BACKEND";                   // See HXHIM_BACKEND_TYPES
const std::string HXHIM_MDHIM_CONFIG                = "MDHIM_CONFIG";              // file path
const std::string HXHIM_LEVELDB_NAME                = "LEVELDB_NAME";              // file path
const std::string HXHIM_LEVELDB_CREATE_IF_MISSING   = "LEVELDB_CREATE_IF_MISSING"; // boolean
const std::string HXHIM_QUEUED_BULK_PUTS            = "QUEUED_BULK_PUTS";          // nonnegative integer
const std::string HXHIM_SUBJECT_TYPE                = "SUBJECT_TYPE";              // See HXHIM_SPO_TYPES
const std::string HXHIM_PREDICATE_TYPE              = "PREDICATE_TYPE";            // See HXHIM_SPO_TYPES
const std::string HXHIM_OBJECT_TYPE                 = "OBJECT_TYPE";               // See HXHIM_SPO_TYPES

const std::string HXHIM_HISTOGRAM_FIRST_N           = "HISTOGRAM_FIRST_N";         // unsigned int
const std::string HXHIM_HISTOGRAM_BUCKET_GEN_METHOD = "HISTOGRAM_BUCKET_METHOD";   // See HXHIM_BUCKET_GENERATORS

/**
 * Set of allowed backends for HXHIM
 */
const std::map<std::string, hxhim_backend_t> HXHIM_BACKENDS = {
    std::make_pair("MDHIM",     HXHIM_BACKEND_MDHIM),
    std::make_pair("LEVELDB",   HXHIM_BACKEND_LEVELDB),
    std::make_pair("IN_MEMORY", HXHIM_BACKEND_IN_MEMORY),
};

/**
 * The allowable subject, predicate, and object types
 */
const std::map<std::string, hxhim_spo_type_t> HXHIM_SPO_TYPES = {
    std::make_pair("INT",    HXHIM_SPO_INT_TYPE),
    std::make_pair("SIZE",   HXHIM_SPO_SIZE_TYPE),
    std::make_pair("FLOAT",  HXHIM_SPO_FLOAT_TYPE),
    std::make_pair("DOUBLE", HXHIM_SPO_DOUBLE_TYPE),
    std::make_pair("BYTE",   HXHIM_SPO_BYTE_TYPE),
};

/**
 * String representations of predefined histogram bucket generators
 */
const std::string TEN_BUCKETS                  = "10_BUCKETS";
const std::string SQUARE_ROOT_CHOICE           = "SQUARE_ROOT_CHOICE";
const std::string STURGES_FORMULA              = "STURGES_FORMULA";
const std::string RICE_RULE                    = "RICE_RULE";
const std::string SCOTTS_NORMAL_REFERENCE_RULE = "SCOTTS_NORMAL_REFERENCE_RULE";
const std::string UNIFORM_LOG2                 = "UNIFORM_LOG2";
const std::string UNIFORM_LOG10                = "UNIFORM_LOG10";

/**
 * Set of predefined histogram bucket generators
 */
const std::map<std::string, Histogram::BucketGen::generator> HXHIM_HISTOGRAM_BUCKET_GENERATORS = {
    std::make_pair(TEN_BUCKETS,                  Histogram::BucketGen::n_buckets),
    std::make_pair(SQUARE_ROOT_CHOICE,           Histogram::BucketGen::square_root_choice),
    std::make_pair(STURGES_FORMULA,              Histogram::BucketGen::sturges_formula),
    std::make_pair(RICE_RULE,                    Histogram::BucketGen::rice_rule),
    std::make_pair(SCOTTS_NORMAL_REFERENCE_RULE, Histogram::BucketGen::scotts_normal_reference_rule),
    std::make_pair(UNIFORM_LOG2,                 Histogram::BucketGen::uniform_logn),
    std::make_pair(UNIFORM_LOG10,                Histogram::BucketGen::uniform_logn),
};

/**
 * Set of predefined extra arguments for bucket generators
 */
const std::size_t two = 2;
const std::size_t ten = 10;
const std::map<std::string, void *> HXHIM_HISTOGRAM_BUCKET_GENERATOR_EXTRA_ARGS = {
    std::make_pair(TEN_BUCKETS,                  (void *) &ten),
    std::make_pair(SQUARE_ROOT_CHOICE,           nullptr),
    std::make_pair(STURGES_FORMULA,              nullptr),
    std::make_pair(RICE_RULE,                    nullptr),
    std::make_pair(SCOTTS_NORMAL_REFERENCE_RULE, nullptr),
    std::make_pair(UNIFORM_LOG2,                 (void *) &two),
    std::make_pair(UNIFORM_LOG10,                (void *) &ten),
};

/**
 * Default configuration
 */
const Config HXHIM_DEFAULT_CONFIG = {
    std::make_pair(HXHIM_BACKEND_TYPE,                  "MDHIM"),
    std::make_pair(HXHIM_MDHIM_CONFIG,                  "mdhim.conf"),
    std::make_pair(HXHIM_LEVELDB_CREATE_IF_MISSING,     "true"),
    std::make_pair(HXHIM_QUEUED_BULK_PUTS,              "5"),
    std::make_pair(HXHIM_SUBJECT_TYPE,                  "BYTE"),
    std::make_pair(HXHIM_PREDICATE_TYPE,                "BYTE"),
    std::make_pair(HXHIM_OBJECT_TYPE,                   "BYTE"),
    std::make_pair(HXHIM_HISTOGRAM_FIRST_N,             "10"),
    std::make_pair(HXHIM_HISTOGRAM_BUCKET_GEN_METHOD,   TEN_BUCKETS),
};

#endif
