#ifndef HXHIM_CONFIG_HPP
#define HXHIM_CONFIG_HPP

#include <string>
#include <unordered_map>

#include "datastore/constants.hpp"
#include "hxhim/constants.h"
#include "hxhim/hash.h"
#include "hxhim/options.h"
#include "transport/constants.hpp"
#include "utils/Configuration.hpp"
#include "utils/Histogram.h"
#include "utils/elen.hpp"
#include "utils/mlogfacs2.h"

namespace hxhim {
namespace config {

/** Constant location where the configuration reader searches */
const std::string CONFIG_FILE                  = "hxhim.conf";

/** A file pointed to by HXHIM_CONFIG will overwrite duplicate values found in CONFIG_FILE (hxhim.conf) */
const std::string CONFIG_ENV                   = "HXHIM_CONFIG";

/**
 * This string prefixes environment variables of respective configuration settings
 * e.g. set the environment variable HXHIM_DEBUG_LEVEL to set the DEBUG_LEVEL value
 * Environment variables are parsed last to allow for easy overwriting of values
 * set by the hard coded configuration file CONFIG_FILE and the configuration file
 * pointed to by HXHIM_CONFIG.
 */
const std::string HXHIM_ENV_NAMESPACE          = "HXHIM_";

/** Configuration Variables */
const std::string DEBUG_LEVEL                  = "DEBUG_LEVEL";                   // See DEBUG_LEVELS

const std::string CLIENT_RATIO                 = "CLIENT_RATIO";                  // positive integer
const std::string SERVER_RATIO                 = "SERVER_RATIO";                  // positive integer
const std::string DATASTORES_PER_SERVER        = "DATASTORES_PER_SERVER";         // positive integer

const std::string DATASTORE_TYPE               = "DATASTORE";                     // See DATASTORE_TYPES
const std::string DATASTORE_PREFIX             = "DATASTORE_PREFIX";              // path
const std::string DATASTORE_BASENAME           = "DATASTORE_BASENAME";            // filename
const std::string DATASTORE_POSTFIX            = "DATASTORE_POSTFIX";             // filename

#if HXHIM_HAVE_LEVELDB
/** LevelDB Datastore Options */
const std::string LEVELDB_CREATE_IF_MISSING    = "LEVELDB_CREATE_IF_MISSING";     // boolean
#endif

#if HXHIM_HAVE_ROCKSDB
/** RocksDB Datastore Options */
const std::string ROCKSDB_CREATE_IF_MISSING    = "ROCKSDB_CREATE_IF_MISSING";     // boolean
#endif

const std::string HASH                         = "HASH";                          // See HASHES

const std::string TRANSPORT                    = "TRANSPORT";                     // See TRANSPORTS

/** MPI Options */
const std::string MPI_LISTENERS                = "NUM_LISTENERS";                 // positive integer

#if HXHIM_HAVE_THALLIUM
/** Thallium Options */
const std::string THALLIUM_MODULE              = "THALLIUM_MODULE";               // See mercury documentation
const std::string THALLIUM_THREAD_COUNT        = "THALLIUM_THREAD_COUNT";         // -1 or greater integer (optional)
#endif

const std::string TRANSPORT_ENDPOINT_GROUP     = "ENDPOINT_GROUP";                // list of ranks or "ALL"

/** Asynchronous PUT Settings */
const std::string START_ASYNC_PUTS_AT          = "START_ASYNC_PUTS_AT";           // nonnegative integer

const std::string MAXIMUM_OPS_PER_REQUEST      = "MAXIMUM_OPS_PER_REQUEST";       // positive integer
const std::string MAXIMUM_SIZE_PER_REQUEST     = "MAXIMUM_SIZE_PER_REQUEST";      // positive integer

/** Histogram Options */
const std::string HISTOGRAM_FIRST_N            = "HISTOGRAM_FIRST_N";             // unsigned int
const std::string HISTOGRAM_BUCKET_GEN_NAME    = "HISTOGRAM_BUCKET_GEN_NAME";     // See HISTOGRAM_BUCKET_GENERATORS
const std::string HISTOGRAM_TRACK_PREDICATES   = "HISTOGRAM_TRACK_PREDICATES";    // comma delimited string
const std::string HISTOGRAM_READ_EXISTING      = "HISTOGRAM_READ_EXISTING";       // boolean
const std::string HISTOGRAM_WRITE_AT_EXIT      = "HISTOGRAM_WRITE_AT_EXIT";       // boolean

/** ELEN Options */
const std::string ELEN_NEG_SYMBOL              = "ELEN_NEG_SYMBOL";               // single character
const std::string ELEN_POS_SYMBOL              = "ELEN_POS_SYMBOL";               // single character > ELEN_NEG_SYMBOL
const std::string ELEN_ENCODE_FLOAT_PRECISION  = "ELEN_ENCODE_FLOAT_PRECISION";   // positive integer
const std::string ELEN_ENCODE_DOUBLE_PRECISION = "ELEN_ENCODE_DOUBLE_PRECISION";  // positive integer > ELEN_ENCODE_FLOAT_PRECISION

/**
 * Set of available debug levels
 */
const std::unordered_map<std::string, int> DEBUG_LEVELS = {
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
const std::unordered_map<std::string, Datastore::Type> DATASTORES = {
    std::make_pair("IN_MEMORY", Datastore::IN_MEMORY),
    #if HXHIM_HAVE_LEVELDB
    std::make_pair("LEVELDB",   Datastore::LEVELDB),
    #endif
    #if HXHIM_HAVE_ROCKSDB
    std::make_pair("ROCKSDB",   Datastore::ROCKSDB),
    #endif
};

/**
 * Set of predefined hash functions
 */
const std::unordered_map<std::string, hxhim_hash_t> HASHES = {
    std::make_pair("DATASTORE_0",         hxhim_hash_DatastoreZero),
    std::make_pair("RANK_MOD_DATASTORES", hxhim_hash_RankModDatastores),
    std::make_pair("SUM_MOD_DATASTORES",  hxhim_hash_SumModDatastores),
    std::make_pair("UTHASH_BER",          hxhim_hash_uthash_BER),
    std::make_pair("UTHASH_SAX",          hxhim_hash_uthash_SAX),
    std::make_pair("UTHASH_FNV",          hxhim_hash_uthash_FNV),
    std::make_pair("UTHASH_OAT",          hxhim_hash_uthash_OAT),
    std::make_pair("UTHASH_JEN",          hxhim_hash_uthash_JEN),
    std::make_pair("UTHASH_SFH",          hxhim_hash_uthash_SFH),
};

/**
 * Set of available transports
 */
const std::unordered_map<std::string, Transport::Type> TRANSPORTS = {
    std::make_pair("NULL",     Transport::TRANSPORT_NULL),
    std::make_pair("MPI",      Transport::TRANSPORT_MPI),
    #if HXHIM_HAVE_THALLIUM
    std::make_pair("THALLIUM", Transport::TRANSPORT_THALLIUM),
    #endif
};

/**
 * Set of predefined histogram bucket generators
 */
const std::unordered_map<std::string, HistogramBucketGenerator_t> HISTOGRAM_BUCKET_GENERATORS = {
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
const std::unordered_map<std::string, void *> HISTOGRAM_BUCKET_GENERATOR_EXTRA_ARGS = {
    std::make_pair("10_BUCKETS",                   (void *) (uintptr_t) (std::size_t) 10),
    std::make_pair("SQUARE_ROOT_CHOICE",           nullptr),
    std::make_pair("STURGES_FORMULA",              nullptr),
    std::make_pair("RICE_RULE",                    nullptr),
    std::make_pair("SCOTTS_NORMAL_REFERENCE_RULE", nullptr),
    std::make_pair("UNIFORM_LOG2",                 (void *) (uintptr_t) (std::size_t) 2),
    std::make_pair("UNIFORM_LOG10",                (void *) (uintptr_t) (std::size_t) 10),
};

/**
 * Default configurations that can be hard coded
 */
const Config::Config DEFAULT_CONFIG = {
    std::make_pair(DEBUG_LEVEL,                   "CRITICAL"),
    std::make_pair(CLIENT_RATIO,                  "2"),
    std::make_pair(SERVER_RATIO,                  "1"),
    std::make_pair(DATASTORES_PER_SERVER,         "1"),
    std::make_pair(DATASTORE_PREFIX,              "."),
    std::make_pair(DATASTORE_BASENAME,            "hxhim"),
    std::make_pair(DATASTORE_POSTFIX,             ""),
#if HXHIM_HAVE_LEVELDB
    std::make_pair(DATASTORE_TYPE,                "LEVELDB"),
    std::make_pair(LEVELDB_CREATE_IF_MISSING,     "true"),
#elif HXHIM_HAVE_ROCKSDB
    std::make_pair(DATASTORE_TYPE,                "ROCKSDB"),
    std::make_pair(ROCKSDB_CREATE_IF_MISSING,     "true"),
#else
    std::make_pair(DATASTORE_TYPE,                "IN_MEMORY"),
#endif
    std::make_pair(TRANSPORT,                     "NULL"),
    std::make_pair(HASH,                          "RANK_MOD_DATASTORES"),
    std::make_pair(TRANSPORT_ENDPOINT_GROUP,      "ALL"),
    std::make_pair(START_ASYNC_PUTS_AT,           "0"),
    std::make_pair(MAXIMUM_OPS_PER_REQUEST,       "128"),
    std::make_pair(MAXIMUM_SIZE_PER_REQUEST,      "1048576"),
    std::make_pair(HISTOGRAM_FIRST_N,             "10"),
    std::make_pair(HISTOGRAM_BUCKET_GEN_NAME,     "10_BUCKETS"),
    std::make_pair(HISTOGRAM_READ_EXISTING,       "true"),
    std::make_pair(HISTOGRAM_WRITE_AT_EXIT,       "true"),
    std::make_pair(ELEN_NEG_SYMBOL,               std::string(elen::NEG_SYMBOL, 1)),
    std::make_pair(ELEN_NEG_SYMBOL,               std::string(elen::POS_SYMBOL, 1)),
    std::make_pair(ELEN_ENCODE_FLOAT_PRECISION,   std::to_string(elen::encode::FLOAT_PRECISION)),
    std::make_pair(ELEN_ENCODE_DOUBLE_PRECISION,  std::to_string(elen::encode::DOUBLE_PRECISION)),
};

}
}

#endif
