#include <sstream>
#include <vector>

#include <gtest/gtest.h>

#include "utils/Configuration.hpp"

// Context for reading configuration from a string
class ConfigOnePair : public Config::Reader {
    public:
        ConfigOnePair(const std::string &src)
            : Config::Reader(),
              src(src)
        {}

        bool process(Config::Config &config) const {
            std::string key, value;
            if (std::stringstream(src) >> key >> value) {
                config[key] = value;
                return true;
            }

            return false;
        }

    private:
        const std::string src;
};

TEST(ConfigSequence, add) {
    Config::Sequence sequence;

    ConfigOnePair first("KEY1 VALUE1");
    EXPECT_EQ(sequence.add(&first), (std::size_t) 1);

    ConfigOnePair second("KEY2 VALUE2");
    EXPECT_EQ(sequence.add(&second), (std::size_t) 2);

    // read the configuration
    Config::Config config;
    sequence.process(config);
    EXPECT_EQ(config.size(), (std::size_t) 2);
    EXPECT_EQ(config.at("KEY1"), "VALUE1");
    EXPECT_EQ(config.at("KEY2"), "VALUE2");
}

TEST(ConfigSequence, reset) {
    Config::Sequence sequence;

    // add to first config reader
    ConfigOnePair first("KEY1 VALUE1");
    EXPECT_EQ(sequence.add(&first), (std::size_t) 1);
    EXPECT_EQ(sequence.reset(), 0);

    // add to second config reader (overwrites first)
    ConfigOnePair second("KEY2 VALUE2");
    EXPECT_EQ(sequence.add(&second), (std::size_t) 1);
    EXPECT_EQ(sequence.reset(), 0);

    // read the configuration
    Config::Config config;

    sequence.process(config);
    EXPECT_EQ(config.size(), (std::size_t) 0);
}

TEST(ConfigSequence, overwrite) {
    Config::Sequence sequence;

    // add to first config reader
    ConfigOnePair first("KEY VALUE1");
    EXPECT_EQ(sequence.add(&first), (std::size_t) 1);

    // add to second config reader (overwrites first)
    ConfigOnePair second("KEY VALUE2");
    EXPECT_EQ(sequence.add(&second), (std::size_t) 2);

    // read the configuration
    Config::Config config;

    sequence.process(config);
    EXPECT_EQ(config.size(), (std::size_t) 1);
    EXPECT_EQ(config.at("KEY"), "VALUE2");
}

TEST(ConfigEnvironmentVar, usage) {
    const std::string env_var = "TEST_ENV_VAR";
    const std::string var     = "TEST_VAR";
    const std::string val     = "VALUE";

    Config::EnvironmentVar cve(env_var, var);

    Config::Config config;

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
