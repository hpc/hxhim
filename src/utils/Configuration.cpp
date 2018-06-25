#include <algorithm>
#include <dirent.h>
#include <fstream>
#include <sstream>
#include <sys/types.h>

#include "Configuration.hpp"

ConfigReader::~ConfigReader() {}

ConfigSequence::ConfigSequence()
  : sequence_(),
    next_index_(0)
{}

/**
 * next_index
 *
 * @return the next index a configuration can be placed at
 */
std::size_t ConfigSequence::next_index() const {
    return next_index_;
}

/**
 * add
 * Add a new source
 *
 * @param func the callback to be used
 * @return the next avaialble index
 */
std::size_t ConfigSequence::add(const ConfigReader *reader) {
    return add(next_index_, reader);
}

/**
 * add
 * Add a source at the given position.
 * The old source will be erased.
 *
 * @param index a specific index to place this source at
 * @param func  the callback to be used
 * @return the  next avaialble index
 */
std::size_t ConfigSequence::add(const std::size_t index, const ConfigReader *reader) {
    sequence_[index] = reader;
    next_index_ = std::max(index + 1, next_index_);
    return next_index_;
}

/**
 * reset
 * Clears the stored ConfigReaders
 *
 * @return next_index_, which is 0
 */
std::size_t ConfigSequence::reset() {
    sequence_.clear();
    return next_index_ = 0;
}

/**
 * Process
 * Try each source, overwriting previously set values.
 *
 * @param config the configuration that will be used by the user
 */
void ConfigSequence::process(Config &config) const {
    config.clear();
    for(std::pair<const std::size_t, const ConfigReader *> const &seq : sequence_) {
        if (seq.second) {
            seq.second->process(config);
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
    int ret = parse_kv_stream(config, f);
    return ret;
}

ConfigFileEnvironment::ConfigFileEnvironment(const std::string& filename)
  : ConfigReader(),
    filename_(filename)
{}

ConfigFileEnvironment::~ConfigFileEnvironment() {}

bool ConfigFileEnvironment::process(Config &config) const {
    char *env = getenv(filename_.c_str());
    if (env) {
        std::ifstream f(env);
        return parse_kv_stream(config, f);
    }
    return false;
}

ConfigVarEnvironment::ConfigVarEnvironment(const std::string &key)
  : ConfigReader(),
    key_(key)
{}

ConfigVarEnvironment::~ConfigVarEnvironment() {}

bool ConfigVarEnvironment::process(Config &config) const {
    char *env = getenv(key_.c_str());
    if (env) {
        config[key_] = env;
        return true;
    }
    return false;
}
