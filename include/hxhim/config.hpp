#ifndef HXHIM_CONFIG_HPP
#define HXHIM_CONFIG_HPP

#include <string>
#include <unordered_map>

#include "datastore/constants.hpp"
#include "hxhim/constants.h"
#include "hxhim/hash.hpp"
#include "hxhim/options.h"
#include "transport/constants.hpp"
#include "utils/Configuration.hpp"
#include "utils/Histogram.h"
#include "utils/mlogfacs2.h"

namespace hxhim {
namespace config {

/**
 * Constant locations where the configuration reader searches
 */
const std::string CONFIG_FILE                 = "hxhim.conf";
const std::string CONFIG_ENV                  = "HXHIM_CONFIG";

const std::string DEBUG_LEVEL                 = "DEBUG_LEVEL";               // See DEBUG_LEVELS

const std::string CLIENT_RATIO                = "CLIENT_RATIO";              // positive integer
const std::string SERVER_RATIO                = "SERVER_RATIO";              // positive integer

const std::string DATASTORES_PER_RANGE_SERVER = "DATASTORES_PER_RS";         // positive integer
const std::string DATASTORE_TYPE              = "DATASTORE";                 // See DATASTORE_TYPES

/** LevelDB Datastore Options */
const std::string LEVELDB_PREFIX              = "LEVELDB_PREFIX";            // string
const std::string LEVELDB_CREATE_IF_MISSING   = "LEVELDB_CREATE_IF_MISSING"; // boolean

const std::string HASH                        = "HASH";                      // See HASHES

const std::string TRANSPORT                   = "TRANSPORT";                 // See TRANSPORTS

/** MPI Options */
const std::string MPI_LISTENERS               = "NUM_LISTENERS";             // positive integer

/** Thallium Options */
const std::string THALLIUM_MODULE             = "THALLIUM_MODULE";           // See mercury documentation

const std::string TRANSPORT_ENDPOINT_GROUP    = "ENDPOINT_GROUP";            // list of ranks or "ALL"

/** Asynchronous PUT Settings */
const std::string START_ASYNC_PUT_AT          = "START_ASYNC_PUT_AT";        // nonnegative integer
const std::string MAXIMUM_OPS_PER_SEND        = "MAXIMUM_OPS_PER_SEND";      // positive integer

/** Histogram Options */
const std::string HISTOGRAM_FIRST_N           = "HISTOGRAM_FIRST_N";         // unsigned int
const std::string HISTOGRAM_BUCKET_GEN_METHOD = "HISTOGRAM_BUCKET_METHOD";   // See HISTOGRAM_BUCKET_GENERATORS

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
const std::unordered_map<std::string, datastore::Type> DATASTORES = {
    #if HXHIM_HAVE_LEVELDB
    std::make_pair("LEVELDB",   datastore::LEVELDB),
    #endif
    std::make_pair("IN_MEMORY", datastore::IN_MEMORY),
};

/**
 * Set of predefined hash functions
 */
const std::unordered_map<std::string, hxhim_hash_t> HASHES = {
    std::make_pair("RANK_0",              hash::RankZero),
    std::make_pair("MY_RANK",             hash::MyRank),
    std::make_pair("RANK_MOD_DATASTORES", hash::RankModDatastores),
    std::make_pair("SUM_MOD_DATASTORES",  hash::SumModDatastores),
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
const std::size_t two = 2;
const std::size_t ten = 10;
const std::unordered_map<std::string, void *> HISTOGRAM_BUCKET_GENERATOR_EXTRA_ARGS = {
    std::make_pair("10_BUCKETS",                   (void *) &ten),
    std::make_pair("SQUARE_ROOT_CHOICE",           nullptr),
    std::make_pair("STURGES_FORMULA",              nullptr),
    std::make_pair("RICE_RULE",                    nullptr),
    std::make_pair("SCOTTS_NORMAL_REFERENCE_RULE", nullptr),
    std::make_pair("UNIFORM_LOG2",                 (void *) &two),
    std::make_pair("UNIFORM_LOG10",                (void *) &ten),
};

/**
 * Default configurations that can be hard coded
 */
const Config DEFAULT_CONFIG = {
    std::make_pair(DEBUG_LEVEL,                   "CRITICAL"),
    std::make_pair(CLIENT_RATIO,                  "2"),
    std::make_pair(SERVER_RATIO,                  "1"),
    std::make_pair(DATASTORES_PER_RANGE_SERVER,   "1"),
    std::make_pair(DATASTORE_TYPE,                "LEVELDB"),
    std::make_pair(LEVELDB_PREFIX,                "."),
    std::make_pair(LEVELDB_CREATE_IF_MISSING,     "true"),
    std::make_pair(TRANSPORT,                     "NULL"),
    std::make_pair(HASH,                          "MY_RANK"),
    std::make_pair(TRANSPORT_ENDPOINT_GROUP,      "ALL"),
    std::make_pair(START_ASYNC_PUT_AT,            "256"),
    std::make_pair(MAXIMUM_OPS_PER_SEND,          "128"),
    std::make_pair(HISTOGRAM_FIRST_N,             "10"),
    std::make_pair(HISTOGRAM_BUCKET_GEN_METHOD,   "10_BUCKETS"),
};

}
}

#endif
