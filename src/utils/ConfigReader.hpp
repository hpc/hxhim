#ifndef CONFIGURATION_READER
#define CONFIGURATION_READER

#include <cmath>
#include <cstdlib>
#include <functional>
#include <map>

/**
 * ConfigReader
 * A series of user defined calbacks are passed
 * into ConfigReader. These callbacks search for
 * the given source in a specified order
 * to allow for preference of source.
 *
 * Each source is assumed to provide a full
 * configuration. Thus, The first callback to
 * return true will terminate parsing and return.
 *
 * Each source is assumed to be accessible through
 * a string (filename, directory name, formatted
 * string, etc.).
 */
class ConfigReader {
    public:
        ConfigReader();

        /** @description Gets the next unused index */
        std::size_t next_index() const;

        /** @description the configuration type */
        typedef std::map<std::string, std::string> Config_t;

        /** @description the signature of callbacks used to set Config_t; the string is an argument for the callback */
        typedef std::function<bool(Config_t &, const std::string&)> ParserFunc_t;

        /** @description Add a new source */
        std::size_t add(const ParserFunc_t &func, const std::string &src);

        /** @description Add a new source at a specific position (overwrites old source) */
        std::size_t add(const std::size_t index, const ParserFunc_t &func, const std::string &src);

        /** @description call all of the configuration parsing functions in the order they are listed */
        bool read(Config_t &config) const;

    private:
        ConfigReader(const ConfigReader&  copy)           = delete;
        ConfigReader(const ConfigReader&& copy)           = delete;
        ConfigReader &operator=(const ConfigReader&  rhs) = delete;
        ConfigReader &operator=(const ConfigReader&& rhs) = delete;

        /** @description A pair for keeping track of the type and source of (a) variable(s) */
        typedef struct FunctionSource {
            FunctionSource(const ParserFunc_t &parser = nullptr, const std::string &source = "")
              : func(parser),
                src(source)
            {}

            ParserFunc_t func;
            std::string src;
        } FunctionSource_t;

        typedef std::map<std::size_t, FunctionSource_t> Order_t;
        Order_t order_;

        std::size_t next_index_;
};

#endif
