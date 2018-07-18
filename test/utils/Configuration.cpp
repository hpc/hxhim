#include <sstream>
#include <vector>

#include <gtest/gtest.h>

#include "utils/Configuration.hpp"

// Context for reading configuration from a string
class ConfigOnePair : public ConfigReader {
    public:
        ConfigOnePair(const std::string &src)
            : ConfigReader(),
              src_(src)
        {}

        bool process(Config &config) const {
            std::string key, value;
            if (std::stringstream(src_) >> key >> value) {
                config[key] = value;
                return true;
            }

            return false;
        }

    private:
        std::string src_;
};

TEST(ConfigSequence, usage) {
    ConfigSequence config_sequence;

    // add to first config reader
    ConfigOnePair first("KEY VALUE1");
    EXPECT_EQ(config_sequence.add(&first), 1);

    // add to second config reader (overwrites first)
    ConfigOnePair second("KEY VALUE2");
    EXPECT_EQ(config_sequence.add(&second), 2);

    // read the configuration
    Config config;

    config_sequence.process(config);
    EXPECT_EQ(config.size(), 1);
    EXPECT_EQ(config.at("KEY"), "VALUE2");
}
