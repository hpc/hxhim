#ifndef CONFIGURATION
#define CONFIGURATION

#include <algorithm>
#include <cmath>
#include <list>
#include <sstream>
#include <string>
#include <unordered_map>


/** @description The underlying configuration type */
typedef std::unordered_map<std::string, std::string> Config;
typedef Config::const_iterator Config_it;

/**
 * ConfigReader
 * The base type for methods of finding, reading, and parsing
 * configuration sources.
 *
 * ConfigReaders should be added to ConfigSequence to be called
 * in the desired order.
 */
class ConfigReader {
    public:
        virtual ~ConfigReader() = 0;

        /** @description This function should be implemented in order to fill in a configuration */
        virtual bool process(Config &config) const = 0;
};

/**
 * ConfigSequence
 * A series of user defined child classes of ConfigReader
 * are passed into ConfigSequence. These classes contain all
 * of the information necessary to search for a configuration
 * and fill in as much of a Config variable as they can. The
 * configurations are read in FIFO order, so newer configurations
 * will over older configurations.
 *
 * ConfigSequence DOES NOT take ownership of the ConfigReaders
 */
class ConfigSequence {
    public:
        ConfigSequence();

        /** @description Add a new source */
        std::size_t add(const ConfigReader *reader);

        /** @description Call all of the configuration parsing functions in the order they are listed */
        void process(Config &config) const;

        /** @description Resets the ConfigSequence for reuse */
        std::size_t reset();

    private:
        ConfigSequence(const ConfigSequence&  copy)           = delete;
        ConfigSequence(const ConfigSequence&& copy)           = delete;
        ConfigSequence &operator=(const ConfigSequence&  rhs) = delete;
        ConfigSequence &operator=(const ConfigSequence&& rhs) = delete;

        typedef std::list<const ConfigReader *> Sequence_t;
        Sequence_t sequence_;
};

/** Example ConfigReaders that try to get configurations from different sources */
/**
 * ConfigFile
 * Attempts to find the file with the given filename
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
 * ConfigFileEnvironment
 * Attempts to find the file pointed to by a given environment variable
 */
class ConfigFileEnvironment : public ConfigReader {
    public:
       ConfigFileEnvironment(const std::string& key);
       ~ConfigFileEnvironment();

       bool process(Config &config) const;

    private:
       const std::string key_;
};

/**
 * ConfigVarEnvironment
 * Attempts to find one configuration variable set as an environment variable
 */
class ConfigVarEnvironment : public ConfigReader {
    public:
       ConfigVarEnvironment(const std::string & env, const std::string &key);
       ~ConfigVarEnvironment();

       bool process(Config &config) const;

    private:
       const std::string env_;
       const std::string key_;
};

#define CONFIG_FOUND     0
#define CONFIG_NOT_FOUND 1
#define CONFIG_ERROR     2

/**
 * get_value
 * Helper function for reading numeric values from the configuration
 *
 * @param config     the configuration
 * @param config_key the entry in the configuraion to read
 * @tparam value     the value of the configuration
 * @return CONFIG_FOUND if the configuration value was good, CONFIG_NOT_FOUND if the configuration key was not found, or CONFIG_ERROR if the configuration value was bad
 */
template <typename T>
int get_value(const Config &config, const std::string &config_key, T &v) {
    // find the key
    Config_it in_config = config.find(config_key);
    if (in_config != config.end()) {
        return (std::stringstream(in_config->second) >> v)?CONFIG_FOUND:CONFIG_ERROR;
    }

    return CONFIG_NOT_FOUND;
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
        typename std::unordered_map<std::string, T>::const_iterator in_map = map.find(in_config->second);
        if (in_map == map.end()) {
            return CONFIG_ERROR;
        }

        value = in_map->second;
        return CONFIG_FOUND;
    }

    return CONFIG_NOT_FOUND;
}

#endif
