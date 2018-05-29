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
 * Try each source until one completes successfully.
 * The configuration is cleared each time.
 * If a pointer is null, it is skipped.
 *
 * @param config the configuration that will be used by the user
 * @return whether or not any of the callbacks returned true
 */
bool ConfigSequence::process(Config &config) const {
    for(std::pair<const std::size_t, const ConfigReader *> const &seq : sequence_) {
        config.clear();
        if (seq.second && seq.second->process(config)) {
            return true;
        }
    }

    return false;
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

ConfigDirectory::ConfigDirectory(const std::string &directory)
  : ConfigReader(),
    directory_(directory)
{}

ConfigDirectory::~ConfigDirectory() {}

bool ConfigDirectory::process(Config &config) const {
    DIR *dirp = opendir(directory_.c_str());
    struct dirent *entry = nullptr;
    while ((entry = readdir(dirp))) {
        // do something with the entry
    }
    closedir(dirp);
    return false;

}

ConfigEnvironment::ConfigEnvironment(const std::string& variable)
  : ConfigReader(),
    variable_(variable)
{}

ConfigEnvironment::~ConfigEnvironment() {}

bool ConfigEnvironment::process(Config &config) const {
    char *env = getenv(variable_.c_str());
    if (env) {
        std::ifstream f(env);
        return parse_kv_stream(config, f);
    }
    return false;
}
