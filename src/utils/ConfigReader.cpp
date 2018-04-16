#include "ConfigReader.hpp"

ConfigReader::ConfigReader()
  : order_(),
    next_index_(0)
{}

/**
 * next_index
 *
 * @return the next index a configuration can be placed at
 */
std::size_t ConfigReader::next_index() const {
    return next_index_;
}

/**
 * add
 * Add a new source
 *
 * @param func the callback to be used
 * @param src  the string that will be used to obtain a source
 * @return the next avaialble index
 */
std::size_t ConfigReader::add(const ParserFunc_t &func, const std::string &src) {
    return add(next_index_, func, src);
}

/**
 * add
 * Add a source at the given position.
 * The old source will be erased.
 *
 * @param index a specific index to place this source at
 * @param func  the callback to be used
 * @param src   the string that will be used to obtain a source
 * @return the  next avaialble index
 */
std::size_t ConfigReader::add(const std::size_t index, const ParserFunc_t &func, const std::string &src) {
    if (!func) {
        return next_index_;
    }

    order_[index].func = func;
    order_[index].src = src;
    next_index_ = std::max(index + 1, next_index_);
    return next_index_;
}

/**
 * read
 * Read each source until one completes successfully.
 * The configuration is cleared each time.
 * If a callback is null, it is skipped.
 *
 * @param config the configuration that will be used by the user
 * @return whether or not any of the callbacks returned true
 */
bool ConfigReader::read(Config_t &config) const {
    for(std::pair<const std::size_t, FunctionSource_t> const &src : order_) {
        config.clear();
        if (src.second.func && src.second.func(config, src.second.src)) {
            return true;
        }
    }

    return false;
}
