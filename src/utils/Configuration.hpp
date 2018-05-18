#ifndef CONFIGURATION
#define CONFIGURATION

#include <cmath>
#include <map>

/** @description The underlying configuration type */
typedef std::map<std::string, std::string> Config;

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
 * of the information necessary to  search for a configuration
 * in a specified order to allow for preference of source.
 *
 * Each source should provide a full configuration.
 * The first callback to return true will terminate
 * search/parsing and return.
 *
 * ConfigSequence DOES NOT take ownership of the ConfigReaders
 */
class ConfigSequence {
    public:
        ConfigSequence();

        /** @description Gets the next unused index */
        std::size_t next_index() const;

        /** @description Add a new source */
        std::size_t add(const ConfigReader *reader);

        /** @description Add a new source at a specific position (overwrites old source) */
        std::size_t add(const std::size_t index, const ConfigReader *reader);

        /** @description Call all of the configuration parsing functions in the order they are listed */
        bool process(Config &config) const;

        /** @description Resets the ConfigSequence for reuse */
        std::size_t reset();

    private:
        ConfigSequence(const ConfigSequence&  copy)           = delete;
        ConfigSequence(const ConfigSequence&& copy)           = delete;
        ConfigSequence &operator=(const ConfigSequence&  rhs) = delete;
        ConfigSequence &operator=(const ConfigSequence&& rhs) = delete;

        typedef std::map<std::size_t, const ConfigReader *> Sequence_t;
        Sequence_t sequence_;

        std::size_t next_index_;
};

#endif
