#include <algorithm>
#include <dirent.h>
#include <fstream>
#include <sstream>
#include <sys/types.h>

#include "utils/Configuration.hpp"

ConfigReader::~ConfigReader() {}

ConfigSequence::ConfigSequence()
  : sequence_()
{}

/**
 * add
 * Add a new source
 *
 * @param func the callback to be used
 * @return the next avaialble index
 */
std::size_t ConfigSequence::add(const ConfigReader *reader) {
    sequence_.push_back(reader);
    return sequence_.size();
}

/**
 * reset
 * Clears the stored ConfigReaders
 *
 * @return 0
 */
std::size_t ConfigSequence::reset() {
    sequence_.clear();
    return sequence_.size();
}

/**
 * Process
 * Try each source, overwriting previously set values.
 * The return value of each seq->process() is ignored
 * so that config sources can not exist and not break
 * this function.
 *
 * @param config the configuration that will be used by the user
 */
void ConfigSequence::process(Config &config) const {
    config.clear();
    for(const ConfigReader * const &seq : sequence_) {
        if (seq) {
            seq->process(config);
        }
    }
}

/**
 * parse_kv_stream
 * Generic key-value stream parser
 *
 * @param config the configuration to fill
 * @param stream the input stream
 * @return how many items were read; 0 is considered a failure even if the file was opened properly
 */
static bool parse_kv_stream(Config &config, std::istream& stream) {
    // parsing should not cross line boundaries
    std::string line;
    while (std::getline(stream, line)) {
        std::stringstream s(line);
        std::string key, value;
        if (s >> key >> value) {
            // check for comments
            if (key[0] == '#') {
                continue;
            }

            // remove trailing whitespace
            std::string::size_type i = value.size();
            if (value.size()) {
                while (std::isspace(value[i - 1])) {
                    i--;
                }
            }

            config[key] = value.substr(0, i);
        }
    }

    return config.size();
}

ConfigFile::ConfigFile(const std::string &filename)
  : ConfigReader(),
    filename_(filename)
{}

ConfigFile::~ConfigFile() {}

bool ConfigFile::process(Config &config) const {
    std::ifstream f(filename_);
    return parse_kv_stream(config, f);
}

ConfigFileEnvironment::ConfigFileEnvironment(const std::string& key)
  : ConfigReader(),
    key_(key)
{}

ConfigFileEnvironment::~ConfigFileEnvironment() {}

bool ConfigFileEnvironment::process(Config &config) const {
    char *env = getenv(key_.c_str());
    if (env) {
        std::ifstream f(env);
        return parse_kv_stream(config, f);
    }
    return false;
}

ConfigVarEnvironment::ConfigVarEnvironment(const std::string &env, const std::string &key)
  : ConfigReader(),
    env_(env),
    key_(key)
{}

ConfigVarEnvironment::~ConfigVarEnvironment() {}

bool ConfigVarEnvironment::process(Config &config) const {
    char *env = getenv(env_.c_str());
    if (env) {
        config[key_] = env;
        return true;
    }
    return false;
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
int get_value(const Config &config, const std::string &config_key, bool &b) {
    // find the key
    Config_it in_config = config.find(config_key);
    if (in_config != config.end()) {
        std::string config_value = in_config->second;
        std::transform(config_value.begin(), config_value.end(), config_value.begin(), ::tolower);
        return (std::stringstream(config_value) >> std::boolalpha >> b)?CONFIG_FOUND:CONFIG_ERROR;
    }

    return CONFIG_NOT_FOUND;
}
