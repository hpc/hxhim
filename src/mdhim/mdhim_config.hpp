#ifndef MDHIM_CONFIG_PRIVATE
#define MDHIM_CONFIG_PRIVATE

#include "mlog2.h"
#include "mlogfacs2.h"

#include "Configuration.hpp"
#include "mdhim_constants.h"
#include "mdhim_options_struct.h"

/**
 * Constant locations where the configuration reader searches
 */
const std::string MDHIM_CONFIG_FILE = "mdhim.conf";
const std::string MDHIM_CONFIG_DIR  = "./";
const std::string MDHIM_CONFIG_ENV  = "MDHIM_CONFIG";

/**
 * Constants used in the mdhim configuration file
 */
const std::string DB_PATH            = "DB_PATH";               // directory
const std::string DB_NAME            = "DB_NAME";               // string
const std::string DB_TYPE            = "DB_TYPE";               // See DB_TYPES
const std::string RSERVER_FACTOR     = "RSERVER_FACTOR";        // positive integer
const std::string MAX_RECS_PER_SLICE = "MAX_RECS_PER_SLICE";    // positive integer
const std::string KEY_TYPE           = "KEY_TYPE";              // See KEY_TYPES
const std::string DEBUG_LEVEL        = "DEBUG_LEVEL";           // See DEBUG_LEVELS
const std::string NUM_WORKER_THREADS = "NUM_WORKER_THREADS";    // positive integer
const std::string MANIFEST_PATH      = "MANIFEST_PATH";         // directory
const std::string CREATE_NEW_DB      = "CREATE_NEW_DB";         // true/false
const std::string DB_WRITE           = "DB_WRITE";              // See DB_WRITES
const std::string DB_HOST            = "DB_HOST";               // string
const std::string DB_LOGIN           = "DB_LOGIN";              // string
const std::string DB_PASSWORD        = "DB_PASSWORD";           // string
const std::string DBS_HOST           = "DBS_HOST";              // string
const std::string DBS_LOGIN          = "DBS_LOGIN";             // string
const std::string DBS_PASSWORD       = "DBS_PASSWORD";          // string
const std::string USE_MPI            = "USE_MPI";               // true/false
const std::string MEMORY_ALLOC_SIZE  = "MEMORY_ALLOC_SIZE";     // positive integer
const std::string MEMORY_REGIONS     = "MEMORY_REGIONS";        // positive integer
const std::string USE_THALLIUM       = "USE_THALLIUM";          // true/false
const std::string THALLIUM_MODULE    = "THALLIUM_MODULE";       // See mercury documentation
const std::string ENDPOINT_GROUP     = "ENDPOINT_GROUP";        // list of globally unique rank ids or "ALL"

/**
 * Mapping from configuration value to database type
 */
const std::map<std::string, int> DB_TYPES = {
    std::make_pair("LEVELDB", LEVELDB),
    std::make_pair("MYSQLDB", MYSQLDB),
    std::make_pair("ROCKSDB", ROCKSDB),
};

/**
 * Mapping from configuration value to key type
 */
const std::map<std::string, int> KEY_TYPES = {
    std::make_pair("INT",      MDHIM_INT_KEY),
    std::make_pair("LONG_INT", MDHIM_LONG_INT_KEY),
    std::make_pair("FLOAT",    MDHIM_FLOAT_KEY),
    std::make_pair("DOUBLE",   MDHIM_DOUBLE_KEY),
    std::make_pair("STRING",   MDHIM_STRING_KEY),
    std::make_pair("BYTE",     MDHIM_BYTE_KEY),
};

/**
 * Mapping from configuration value to debug level
 */
const std::map<std::string, int> DEBUG_LEVELS = {
    std::make_pair("EMERGENCY", MLOG_EMERG),
    std::make_pair("ALERT",     MLOG_ALERT),
    std::make_pair("CRITICAL",  MLOG_CRIT),
    std::make_pair("ERROR",     MLOG_ERR),
    std::make_pair("WARNING",   MLOG_WARN),
    std::make_pair("NOTICE",    MLOG_NOTE),
    std::make_pair("INFO",      MLOG_INFO),
};

/**
 * Mapping from configuration value to database write type
 */
const std::map<std::string, int> DB_WRITES = {
    std::make_pair("OVERWRITE", MDHIM_DB_OVERWRITE),
    std::make_pair("APPEND",    MDHIM_DB_APPEND),
};

/**
 * Default configuration
 */
const Config MDHIM_DEFAULT_CONFIG = {
    std::make_pair(DB_PATH,            "./"),
    std::make_pair(DB_NAME,            "mdhimTstDB-"),
    std::make_pair(DB_TYPE,            "LEVELDB"),
    std::make_pair(RSERVER_FACTOR,     "1"),
    std::make_pair(MAX_RECS_PER_SLICE, "1000"),
    std::make_pair(KEY_TYPE,           "BYTE"),
    std::make_pair(DEBUG_LEVEL,        "CRITICAL"),
    std::make_pair(NUM_WORKER_THREADS, "1"),
    std::make_pair(MANIFEST_PATH,      "./"),
    std::make_pair(CREATE_NEW_DB,      "true"),
    std::make_pair(DB_WRITE,           "OVERWRITE"),
    std::make_pair(DB_HOST,            "localhost"),
    std::make_pair(DB_LOGIN,           "test"),
    std::make_pair(DB_PASSWORD,        "pass"),
    std::make_pair(DBS_HOST,           "localhost"),
    std::make_pair(DBS_LOGIN,          "test"),
    std::make_pair(DBS_PASSWORD,       "pass"),
    std::make_pair(USE_MPI,            "true"),
    std::make_pair(MEMORY_ALLOC_SIZE,  "128"),
    std::make_pair(MEMORY_REGIONS,     "256"),
    std::make_pair(USE_THALLIUM,       "false"),
    std::make_pair(THALLIUM_MODULE,    "na+sm"),
    std::make_pair(ENDPOINT_GROUP,     "ALL"),
};

/**
 * ConfigFile
 * Attempts to find the file with the given filename
 * Used in the default configuration reader.
 */
class ConfigFile : public ConfigReader {
    public:
       ConfigFile(const std::string &filename);
       ~ConfigFile();

       bool process(Config &config) const;

    private:
        std::string filename_;
};

/**
 * ConfigDirectory
 * Searches the given directory
 * Used in the default configuration reader.
 */
class ConfigDirectory : public ConfigReader {
    public:
       ConfigDirectory(const std::string &directory);
       ~ConfigDirectory();

       bool process(Config &config) const;

    private:
        std::string directory_;
};

/**
 * ConfigEnvironment
 * Attempts to find the file pointed to by a given environment variable
 * Used in the default configuration reader.
 */
class ConfigEnvironment : public ConfigReader {
    public:
       ConfigEnvironment(const std::string& variable);
       ~ConfigEnvironment();

       bool process(Config &config) const;

    private:
        std::string variable_;
};

/**
 * This function should only be called at
 * the end of a custom configuration reader.
 */
int process_config_and_fill_options(ConfigSequence &config_sequence, mdhim_options_t *opts);

#endif
