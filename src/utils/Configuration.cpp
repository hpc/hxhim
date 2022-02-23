#include <algorithm>
#include <fstream>
#include <sstream>

#include "utils/Configuration.hpp"

Config::Reader::~Reader() {}

Config::Sequence::Sequence()
  : sequence_()
{}

/**
 * add
 * Add a new source
 *
 * @param func the callback to be used
 * @return the next avaialble index
 */
std::size_t Config::Sequence::add(const Reader *reader) {
    sequence_.push_back(reader);
    return sequence_.size();
}

/**
 * reset
 * Clears the stored Readers
 *
 * @return 0
 */
std::size_t Config::Sequence::reset() {
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
void Config::Sequence::process(Config &config) const {
    config.clear();
    for(const Reader * const &seq : sequence_) {
        if (seq) {
            seq->process(config);
        }
    }
}

/**
 * parse_kv_stream
 * Generic key-value stream line parser
 * The first word is the key.
 * The value is everything after the key, with leading and trailing whitespace removed
 *
 * @param config the configuration to fill
 * @param stream the input stream
 * @return how many items were read; 0 is considered a failure even if the file was opened properly
 */
bool parse_kv_stream(Config::Config &config, std::istream &stream) {
    // parsing should not cross line boundaries
    std::string line;
    size_t count = 0;
    while (std::getline(stream, line)) {
        std::string key;
        std::stringstream s(line);
        if (s >> key) {
            // check for comments
            if (key[0] == '#') {
                continue;
            }

            // read everything after first word
            std::string value;
            std::getline(s, value);

            // remove leading whitespace
            std::size_t lhs = 0;
            while ((lhs < value.size()) && std::isspace(value[lhs])) {
                lhs++;
            }

            // remove trailing whitespace
            std::string::size_type rhs = value.size();
            while (rhs && std::isspace(value[rhs - 1])) {
                rhs--;
            }

            config[key] = value.substr(lhs, rhs - lhs);
            count++;
        }
    }

    return !!count;
}

Config::File::File(const std::string &filename)
  : Reader(),
    filename_(filename)
{}

Config::File::~File() {}

bool Config::File::process(Config &config) const {
    std::ifstream f(filename_);
    return parse_kv_stream(config, f);
}

Config::EnvironmentFile::EnvironmentFile(const std::string& key)
  : Reader(),
    key_(key)
{}

Config::EnvironmentFile::~EnvironmentFile() {}

bool Config::EnvironmentFile::process(Config &config) const {
    char *env = getenv(key_.c_str());
    if (env) {
        std::ifstream f(env);
        return parse_kv_stream(config, f);
    }
    return false;
}

Config::EnvironmentVar::EnvironmentVar(const std::string &env, const std::string &key)
  : Reader(),
    env_(env),
    key_(key)
{}

Config::EnvironmentVar::~EnvironmentVar() {}

bool Config::EnvironmentVar::process(Config &config) const {
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
 * @return Config::FOUND if the configuration value was good, Config::NOT_FOUND if the configuration key was not found, or Config::ERROR if the configuration value was bad
 */
int Config::get_value(const Config &config, const std::string &config_key, bool &b) {
    // find the key
    Config_it in_config = config.find(config_key);
    if (in_config != config.end()) {
        std::string config_value = in_config->second;
        std::transform(config_value.begin(), config_value.end(), config_value.begin(), ::tolower);
        return (std::stringstream(config_value) >> std::boolalpha >> b)?FOUND:ERROR;
    }

    return NOT_FOUND;
}
