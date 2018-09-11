#ifndef HXHIM_CONFIG_HPP
#define HXHIM_CONFIG_HPP

#include <map>
#include <string>

#include "datastore/constants.h"
#include "hxhim/constants.h"
#include "hxhim/hash.hpp"
#include "transport/constants.hpp"
#include "utils/Configuration.hpp"
#include "utils/Histogram.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

/**
 * Constant locations where the configuration reader searches
 */
const std::string HXHIM_CONFIG_FILE                 = "hxhim.conf";
const std::string HXHIM_CONFIG_ENV                  = "HXHIM_CONFIG";

const std::string HXHIM_DEBUG_LEVEL                 = "DEBUG_LEVEL";               // See HXHIM_DEBUG_LEVELS

const std::string HXHIM_DATASTORES_PER_RANGE_SERVER = "DATASTORES_PER_RS";         // positive integer
const std::string HXHIM_DATASTORE_TYPE              = "DATASTORE";                 // See HXHIM_DATASTORE_TYPES

/** LevelDB Datastore Options */
const std::string HXHIM_LEVELDB_NAME                = "LEVELDB_NAME";              // file path
const std::string HXHIM_LEVELDB_CREATE_IF_MISSING   = "LEVELDB_CREATE_IF_MISSING"; // boolean

const std::string HXHIM_QUEUED_BULK_PUTS            = "QUEUED_BULK_PUTS";          // nonnegative integer

const std::string HXHIM_HASH                        = "HASH";                      // See HXHIM_HASHES
const std::string HXHIM_TRANSPORT                   = "TRANSPORT";                 // See HXHIM_TRANSPORTS

/** MPI Options */
const std::string HXHIM_MPI_LISTENERS               = "NUM_LISTENERS";             // positive integer

/** Thallium Options */
const std::string HXHIM_THALLIUM_MODULE             = "THALLIUM_MODULE";           // See mercury documentation

const std::string HXHIM_TRANSPORT_ENDPOINT_GROUP    = "ENDPOINT_GROUP";            // list of ranks or "ALL"

/** Histogram Options */
const std::string HXHIM_HISTOGRAM_FIRST_N           = "HISTOGRAM_FIRST_N";         // unsigned int
const std::string HXHIM_HISTOGRAM_BUCKET_GEN_METHOD = "HISTOGRAM_BUCKET_METHOD";   // See HXHIM_BUCKET_GENERATORS

/** Memory Pool Options */
const std::string HXHIM_PACKED_NAME                 = "PACKED_NAME";               // string
const std::string HXHIM_PACKED_ALLOC_SIZE           = "PACKED_ALLOC_SIZE";         // positive integer
const std::string HXHIM_PACKED_REGIONS              = "PACKED_REGIONS";            // positive integer
const std::string HXHIM_BUFFERS_NAME                = "BUFFERS_NAME";              // string
const std::string HXHIM_BUFFERS_ALLOC_SIZE          = "BUFFERS_ALLOC_SIZE";        // positive integer
const std::string HXHIM_BUFFERS_REGIONS             = "BUFFERS_REGIONS";           // positive integer
const std::string HXHIM_BULKS_NAME                  = "BULKS_NAME";                // string
const std::string HXHIM_BULKS_ALLOC_SIZE            = "BULKS_ALLOC_SIZE";          // positive integer
const std::string HXHIM_BULKS_REGIONS               = "BULKS_REGIONS";             // positive integer
const std::string HXHIM_KEYS_NAME                   = "KEYS_NAME";                 // string
const std::string HXHIM_KEYS_ALLOC_SIZE             = "KEYS_ALLOC_SIZE";           // positive integer
const std::string HXHIM_KEYS_REGIONS                = "KEYS_REGIONS";              // positive integer
const std::string HXHIM_ARRAYS_NAME                 = "ARRAYS_NAME";               // string
const std::string HXHIM_ARRAYS_ALLOC_SIZE           = "ARRAYS_ALLOC_SIZE";         // positive integer
const std::string HXHIM_ARRAYS_REGIONS              = "ARRAYS_REGIONS";            // positive integer
const std::string HXHIM_REQUESTS_NAME               = "REQUESTS_NAME";             // string
const std::string HXHIM_REQUESTS_ALLOC_SIZE         = "REQUESTS_ALLOC_SIZE";       // positive integer
const std::string HXHIM_REQUESTS_REGIONS            = "REQUESTS_REGIONS";          // positive integer
const std::string HXHIM_RESPONSES_NAME              = "RESPONSES_NAME";            // string
const std::string HXHIM_RESPONSES_ALLOC_SIZE        = "RESPONSES_ALLOC_SIZE";      // positive integer
const std::string HXHIM_RESPONSES_REGIONS           = "RESPONSES_REGIONS";         // positive integer
const std::string HXHIM_RESULT_NAME                 = "RESULT_NAME";               // string
const std::string HXHIM_RESULT_ALLOC_SIZE           = "RESULT_ALLOC_SIZE";         // positive integer
const std::string HXHIM_RESULT_REGIONS              = "RESULT_REGIONS";            // positive integer
const std::string HXHIM_RESULTS_NAME                = "RESULTS_NAME";              // string
const std::string HXHIM_RESULTS_ALLOC_SIZE          = "RESULTS_ALLOC_SIZE";        // positive integer
const std::string HXHIM_RESULTS_REGIONS             = "RESULTS_REGIONS";           // positive integer

/**
 * Set of available debug levels
 */
const std::map<std::string, int> HXHIM_DEBUG_LEVELS = {
    std::make_pair("EMERGENCY", MLOG_EMERG),
    std::make_pair("ALERT",     MLOG_ALERT),
    std::make_pair("CRITICAL",  MLOG_CRIT),
    std::make_pair("ERROR",     MLOG_ERR),
    std::make_pair("WARNING",   MLOG_WARN),
    std::make_pair("NOTICE",    MLOG_NOTE),
    std::make_pair("INFO",      MLOG_INFO),
    std::make_pair("DEBUG",     MLOG_DBG),
    std::make_pair("DEBUG0",    MLOG_DBG0),
    std::make_pair("DEBUG1",    MLOG_DBG1),
    std::make_pair("DEBUG2",    MLOG_DBG2),
    std::make_pair("DEBUG3",    MLOG_DBG3),
};

/**
 * Set of allowed datastores for HXHIM
 */
const std::map<std::string, hxhim_datastore_t> HXHIM_DATASTORES = {
    #if HXHIM_HAVE_LEVELDB
    std::make_pair("LEVELDB",   HXHIM_DATASTORE_LEVELDB),
    #endif
    std::make_pair("IN_MEMORY", HXHIM_DATASTORE_IN_MEMORY),
};

/**
 * Set of predefined hash functions
 */
const std::map<std::string, hxhim_hash_t> HXHIM_HASHES = {
    std::make_pair("RANK",               hxhim::hash::Rank),
    std::make_pair("SUM_MOD_DATASTORES", hxhim::hash::SumModDatastores),
    std::make_pair("LOCAL",              hxhim::hash::Local),
};

/**
 * Set of available transports
 */
const std::map<std::string, Transport::Type> HXHIM_TRANSPORTS = {
    std::make_pair("NULL",     Transport::TRANSPORT_NULL),
    std::make_pair("MPI",      Transport::TRANSPORT_MPI),
    #if HXHIM_HAVE_THALLIUM
    std::make_pair("THALLIUM", Transport::TRANSPORT_THALLIUM),
    #endif
};

/**
 * Set of predefined histogram bucket generators
 */
const std::map<std::string, HistogramBucketGenerator_t> HXHIM_HISTOGRAM_BUCKET_GENERATORS = {
    std::make_pair("10_BUCKETS",                   histogram_n_buckets),
    std::make_pair("SQUARE_ROOT_CHOICE",           histogram_square_root_choice),
    std::make_pair("STURGES_FORMULA",              histogram_sturges_formula),
    std::make_pair("RICE_RULE",                    histogram_rice_rule),
    std::make_pair("SCOTTS_NORMAL_REFERENCE_RULE", histogram_scotts_normal_reference_rule),
    std::make_pair("UNIFORM_LOG2",                 histogram_uniform_logn),
    std::make_pair("UNIFORM_LOG10",                histogram_uniform_logn),
};

/**
 * Set of predefined extra arguments for bucket generators
 */
const std::size_t two = 2;
const std::size_t ten = 10;
const std::map<std::string, void *> HXHIM_HISTOGRAM_BUCKET_GENERATOR_EXTRA_ARGS = {
    std::make_pair("10_BUCKETS",                   (void *) &ten),
    std::make_pair("SQUARE_ROOT_CHOICE",           nullptr),
    std::make_pair("STURGES_FORMULA",              nullptr),
    std::make_pair("RICE_RULE",                    nullptr),
    std::make_pair("SCOTTS_NORMAL_REFERENCE_RULE", nullptr),
    std::make_pair("UNIFORM_LOG2",                 (void *) &two),
    std::make_pair("UNIFORM_LOG10",                (void *) &ten),
};

/**
 * Default configuration
 */
const Config HXHIM_DEFAULT_CONFIG = {
    std::make_pair(HXHIM_DEBUG_LEVEL,                   "CRITICAL"),
    std::make_pair(HXHIM_DATASTORES_PER_RANGE_SERVER,   "1"),
    std::make_pair(HXHIM_DATASTORE_TYPE,                "LEVELDB"),
    std::make_pair(HXHIM_LEVELDB_NAME,                  "leveldb"),
    std::make_pair(HXHIM_LEVELDB_CREATE_IF_MISSING,     "true"),
    std::make_pair(HXHIM_QUEUED_BULK_PUTS,              "5"),
    std::make_pair(HXHIM_TRANSPORT,                     "NULL"),
    std::make_pair(HXHIM_HASH,                          "LOCAL"),
    std::make_pair(HXHIM_TRANSPORT_ENDPOINT_GROUP,      "ALL"),
    std::make_pair(HXHIM_HISTOGRAM_FIRST_N,             "10"),
    std::make_pair(HXHIM_HISTOGRAM_BUCKET_GEN_METHOD,   "10_BUCKETS"),
    std::make_pair(HXHIM_PACKED_NAME,                   "Packed"),
    std::make_pair(HXHIM_PACKED_ALLOC_SIZE,             "1000"),
    std::make_pair(HXHIM_PACKED_REGIONS,                "1000"),
    std::make_pair(HXHIM_BUFFERS_NAME,                  "Buffers"),
    std::make_pair(HXHIM_BUFFERS_ALLOC_SIZE,            "1000"),
    std::make_pair(HXHIM_BUFFERS_REGIONS,               "1000"),
    std::make_pair(HXHIM_BULKS_NAME,                    "Bulks"),
    // std::make_pair(HXHIM_BULKS_ALLOC_SIZE,              "1000"),
    std::make_pair(HXHIM_BULKS_REGIONS,                 "1000"),
    std::make_pair(HXHIM_KEYS_NAME,                     "Keys"),
    std::make_pair(HXHIM_KEYS_ALLOC_SIZE,               "1000"),
    std::make_pair(HXHIM_KEYS_REGIONS,                  "1000"),
    std::make_pair(HXHIM_ARRAYS_NAME,                   "Arrays"),
    std::make_pair(HXHIM_ARRAYS_ALLOC_SIZE,             "1000"),
    std::make_pair(HXHIM_ARRAYS_REGIONS,                "1000"),
    std::make_pair(HXHIM_REQUESTS_NAME,                 "Requests"),
    // std::make_pair(HXHIM_REQUESTS_ALLOC_SIZE,           "1000"),
    std::make_pair(HXHIM_REQUESTS_REGIONS,              "1000"),
    std::make_pair(HXHIM_RESPONSES_NAME,                "Responses"),
    // std::make_pair(HXHIM_RESPONSES_ALLOC_SIZE,          "1000"),
    std::make_pair(HXHIM_RESPONSES_REGIONS,             "1000"),
    std::make_pair(HXHIM_RESULT_NAME,                   "Result"),
    std::make_pair(HXHIM_RESULT_ALLOC_SIZE,             "1000"),
    std::make_pair(HXHIM_RESULT_REGIONS,                "1000"),
    std::make_pair(HXHIM_RESULTS_NAME,                  "Results"),
    // std::make_pair(HXHIM_RESULTS_ALLOC_SIZE,            "1000"),
    // std::make_pair(HXHIM_RESULTS_REGIONS,               "1000"),
};

#endif
