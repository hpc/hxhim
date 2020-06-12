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

TEST(ConfigSequence, add) {
    ConfigSequence config_sequence;

    // add to first config reader
    ConfigOnePair first("KEY1 VALUE1");
    EXPECT_EQ(config_sequence.add(&first), (std::size_t) 1);

    // add to second config reader (overwrites first)
    ConfigOnePair second("KEY2 VALUE2");
    EXPECT_EQ(config_sequence.add(&second), (std::size_t) 2);

    // read the configuration
    Config config;

    config_sequence.process(config);
    EXPECT_EQ(config.size(), (std::size_t) 2);
    EXPECT_EQ(config.at("KEY1"), "VALUE1");
    EXPECT_EQ(config.at("KEY2"), "VALUE2");
}

TEST(ConfigSequence, reset) {
    ConfigSequence config_sequence;

    // add to first config reader
    ConfigOnePair first("KEY1 VALUE1");
    EXPECT_EQ(config_sequence.add(&first), (std::size_t) 1);
    EXPECT_EQ(config_sequence.reset(), 0);

    // add to second config reader (overwrites first)
    ConfigOnePair second("KEY2 VALUE2");
    EXPECT_EQ(config_sequence.add(&second), (std::size_t) 1);
    EXPECT_EQ(config_sequence.reset(), 0);

    // read the configuration
    Config config;

    config_sequence.process(config);
    EXPECT_EQ(config.size(), (std::size_t) 0);
}

TEST(ConfigSequence, overwrite) {
    ConfigSequence config_sequence;

    // add to first config reader
    ConfigOnePair first("KEY VALUE1");
    EXPECT_EQ(config_sequence.add(&first), (std::size_t) 1);

    // add to second config reader (overwrites first)
    ConfigOnePair second("KEY VALUE2");
    EXPECT_EQ(config_sequence.add(&second), (std::size_t) 2);

    // read the configuration
    Config config;

    config_sequence.process(config);
    EXPECT_EQ(config.size(), (std::size_t) 1);
    EXPECT_EQ(config.at("KEY"), "VALUE2");
}

TEST(ConfigVarEnvironment, usage) {
    const std::string env_var = "TEST_ENV_VAR";
    const std::string var     = "TEST_VAR";
    const std::string val     = "VALUE";

    ConfigVarEnvironment cve(env_var, var);

    Config config;

    // remove the variable from the environment if it already exists
    // doesn't affect anything unless the test executable is sourced,
    // which should not be done
    EXPECT_EQ(unsetenv(env_var.c_str()), 0);

    // did not get anything
    EXPECT_EQ(cve.process(config), false);

    // set the environment variable
    EXPECT_EQ(setenv(env_var.c_str(), val.c_str(), 1), 0);

    // get the value from the environment
    // val should be stored in var, not env_var
    EXPECT_EQ(cve.process(config), true);
    EXPECT_EQ(config.at(var), val);
}
