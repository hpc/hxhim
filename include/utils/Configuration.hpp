#ifndef CONFIGURATION
#define CONFIGURATION

#include <list>
#include <sstream>
#include <string>
#include <unordered_map>

#include "utils/macros.hpp"

namespace Config {

const int FOUND     = 0;
const int NOT_FOUND = 1;
const int ERROR     = 2;

/** @description The underlying configuration type */
typedef std::unordered_map<std::string, std::string> Config;
typedef Config::const_iterator Config_it;

/**
 * Reader
 * The base type for methods of finding, reading, and parsing
 * configuration sources.
 *
 * Readers should be added to Sequence to be called
 * in the desired order.
 */
class Reader {
    public:
        virtual ~Reader() = 0;

        /** @description This function should be implemented in order to fill in a configuration */
        virtual bool process(Config &config) const = 0;
};

/**
 * Sequence
 * A series of user defined child classes of Reader
 * are passed into Sequence. These classes contain all
 * of the information necessary to search for a configuration
 * and fill in as much of a Config variable as they can. The
 * configurations are read in FIFO order, so newer configurations
 * will over older configurations.
 *
 * Sequence DOES NOT take ownership of the Readers
 */
class Sequence {
    public:
        Sequence();

        /** @description Add a new source */
        std::size_t add(const Reader *reader);

        /** @description Call all of the configuration parsing functions in the order they are listed */
        void process(Config &config) const;

        /** @description Resets the Sequence for reuse */
        std::size_t reset();

    private:
        Sequence(const Sequence&  copy)           = delete;
        Sequence(const Sequence&& copy)           = delete;
        Sequence &operator=(const Sequence&  rhs) = delete;
        Sequence &operator=(const Sequence&& rhs) = delete;

        typedef std::list<const Reader *> Sequence_t;
        Sequence_t sequence_;
};

/** Example Readers that try to get configurations from different sources */
/**
 * File
 * Attempts to find the file with the given filename
 */
class File : public Reader {
    public:
       File(const std::string &filename);
       ~File();

       bool process(Config &config) const;

    private:
        std::string filename_;
};

/**
 * Directory
 * Searches the given directory
 */
class Directory : public Reader {
    public:
       Directory(const std::string &directory);
       ~Directory();

       bool process(Config &config) const;

    private:
        std::string directory_;
};

/**
 * EnvironmentFile
 * Attempts to find the file pointed to by a given environment variable
 */
class EnvironmentFile : public Reader {
    public:
       EnvironmentFile(const std::string& key);
       ~EnvironmentFile();

       bool process(Config &config) const;

    private:
       const std::string key_;
};

/**
 * EnvironmentVar
 * Attempts to find one configuration variable set as an environment variable
 */
class EnvironmentVar : public Reader {
    public:
       EnvironmentVar(const std::string & env, const std::string &key);
       ~EnvironmentVar();

       bool process(Config &config) const;

    private:
       const std::string env_;
       const std::string key_;
};

/**
 * get_value <std::string> overload
 * Helper function for reading strings from the configuration
 * String configuration values might have whitespace, so don't parse them
 *
 * @param config     the configuration
 * @param config_key the entry in the configuraion to read
 * @param str        the value of the configuration
 * @return Config::FOUND if the configuration key was found, or Config::ERROR if the configuration key was not found
 */
int get_value(const Config &config, const std::string &config_key, std::string &str);

/**
 * get_value
 * Helper function for reading numeric values from the configuration
 *
 * @param config     the configuration
 * @param config_key the entry in the configuraion to read
 * @tparam value     the value of the configuration
 * @return FOUND if the configuration value was good, NOT_FOUND if the configuration key was not found, or ERROR if the configuration value was bad
 */
template <typename T>
int get_value(const Config &config, const std::string &config_key, T &v) {
    // find the key
    Config_it in_config = config.find(config_key);
    if (in_config != config.end()) {
        return (std::stringstream(in_config->second) >> v)?FOUND:ERROR;
    }

    return NOT_FOUND;
}

/**
 * get_value <bool> overload
 * Helper function for reading booleans from the configuration
 *
 * @param config     the configuration
 * @param config_key the entry in the configuraion to read
 * @param b          the value of the configuration
 * @return CONFIG_FOUND if the configuration value was good, CONFIG_NOT_FOUND if the configuration key was not found, or CONFIG_ERROR if the configuration value was bad
 */
int get_value(const Config &config, const std::string &config_key, bool &b);

/**
 * get_from_map
 * Helper function for getting values from a map with the value read from the configuration
 *
 * @param config     the configuration
 * @param config_key the entry in the configuraion to read
 * @tparam map       the map where the config_key should be found
 * @tparam value     the value of the configuration
 * @return CONFIG_FOUND if the configuration value was good, CONFIG_NOT_FOUND if the configuration key was not found, or CONFIG_ERROR if the configuration value was bad
 */
template <typename T>
int get_from_map(const Config &config, const std::string &config_key,
                 const std::unordered_map<std::string, T> &map, T &value) {
    // find key in configuration
    Config_it in_config = config.find(config_key);

    if (in_config != config.end()) {
        // use value to get internal value from map
        REF(map)::const_iterator in_map = map.find(in_config->second);
        if (in_map == map.end()) {
            return ERROR;
        }

        value = in_map->second;
        return FOUND;
    }

    return NOT_FOUND;
}

}

#endif
