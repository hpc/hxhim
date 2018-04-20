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
