#ifndef HXHIM_CONFIG_HPP
#define HXHIM_CONFIG_HPP

#include <map>
#include <string>

#include "hxhim/constants.h"
#include "transport/constants.h"
#include "utils/Configuration.hpp"
#include "utils/Histogram.hpp"
#include "hxhim/hash.hpp"

/**
 * Constant locations where the configuration reader searches
 */
const std::string HXHIM_CONFIG_FILE                 = "hxhim.conf";
const std::string HXHIM_CONFIG_ENV                  = "HXHIM_CONFIG";

const std::string HXHIM_DATASTORE_TYPE              = "DATASTORE";                 // See HXHIM_DATASTORE_TYPES
const std::string HXHIM_DATASTORES_PER_RANGE_SERVER = "DATASTORES_PER_RS";         // positive integer

/** LevelDB Datastore Options*/
const std::string HXHIM_LEVELDB_NAME                = "LEVELDB_NAME";              // file path
const std::string HXHIM_LEVELDB_CREATE_IF_MISSING   = "LEVELDB_CREATE_IF_MISSING"; // boolean

const std::string HXHIM_QUEUED_BULK_PUTS            = "QUEUED_BULK_PUTS";          // nonnegative integer

const std::string HXHIM_HASH                        = "HASH";                      // See HXHIM_HASHES
const std::string HXHIM_TRANSPORT                   = "TRANSPORT";                 // See HXHIM_TRANSPORTS

/** MPI Options */
const std::string HXHIM_MPI_MEMORY_ALLOC_SIZE       = "MEMORY_ALLOC_SIZE";         // positive integer
const std::string HXHIM_MPI_MEMORY_REGIONS          = "MEMORY_REGIONS";            // positive integer
const std::string HXHIM_MPI_LISTENERS               = "NUM_LISTENERS";             // positive integer

/** Thallium Options */
const std::string HXHIM_THALLIUM_MODULE             = "THALLIUM_MODULE";           // See mercury documentation

const std::string HXHIM_TRANSPORT_ENDPOINT_GROUP    = "ENDPOINT_GROUP";            // list of ranks or "ALL"

/** Histogram Options */
const std::string HXHIM_HISTOGRAM_FIRST_N           = "HISTOGRAM_FIRST_N";         // unsigned int
const std::string HXHIM_HISTOGRAM_BUCKET_GEN_METHOD = "HISTOGRAM_BUCKET_METHOD";   // See HXHIM_BUCKET_GENERATORS

/**
 * Set of allowed datastores for HXHIM
 */
const std::map<std::string, hxhim_datastore_t> HXHIM_DATASTORES = {
    std::make_pair("LEVELDB",   HXHIM_DATASTORE_LEVELDB),
    std::make_pair("IN_MEMORY", HXHIM_DATASTORE_IN_MEMORY),
};

/**
 * String representations of predefined hash algorithms
 */
const std::string RANK               = "RANK";
const std::string SUM_MOD_DATASTORES = "SUM_MOD_DATASTORES";

/**
 * Set of predefined hash functions
 */
const std::map<std::string, hxhim::hash::Func> HXHIM_HASHES = {
    std::make_pair(RANK,               hxhim::hash::Rank),
    std::make_pair(SUM_MOD_DATASTORES, hxhim::hash::SumModDatastores),
};

/**
 * Set of available transports
 */
const std::map<std::string, Transport::Type> HXHIM_TRANSPORTS = {
    std::make_pair("MPI",      Transport::TRANSPORT_MPI),
    std::make_pair("THALLIUM", Transport::TRANSPORT_THALLIUM),
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
    std::make_pair(HXHIM_DATASTORE_TYPE,                "LEVELDB"),
    std::make_pair(HXHIM_DATASTORES_PER_RANGE_SERVER,   "1"),
    std::make_pair(HXHIM_LEVELDB_NAME,                  "leveldb"),
    std::make_pair(HXHIM_LEVELDB_CREATE_IF_MISSING,     "true"),
    std::make_pair(HXHIM_HASH,                          SUM_MOD_DATASTORES),
    std::make_pair(HXHIM_QUEUED_BULK_PUTS,              "5"),
    std::make_pair(HXHIM_TRANSPORT,                     "THALLIUM"),
    std::make_pair(HXHIM_THALLIUM_MODULE,               "na+sm"),
    std::make_pair(HXHIM_TRANSPORT_ENDPOINT_GROUP,      "ALL"),
    std::make_pair(HXHIM_HISTOGRAM_FIRST_N,             "10"),
    std::make_pair(HXHIM_HISTOGRAM_BUCKET_GEN_METHOD,   TEN_BUCKETS),
};

#endif
