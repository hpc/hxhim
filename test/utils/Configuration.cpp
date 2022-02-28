#include <sstream>
#include <unordered_map>

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

// defined in src/utils/Configuration.cpp
bool parse_kv_stream(Config::Config &config, std::istream &stream);

TEST(Config, parse_kv_stream) {
    Config::Config config;

    // value with space in the middle and trailing whitespace
    std::stringstream s1("KEY1   VALUE1  VALUE2    ");
    EXPECT_EQ(parse_kv_stream(config, s1), true);
    EXPECT_EQ(config.at("KEY1"), "VALUE1  VALUE2");

    // key with no value
    std::stringstream s2("KEY2    ");
    EXPECT_EQ(parse_kv_stream(config, s2), true);
    EXPECT_EQ(config.at("KEY2"), "");
}

TEST(Config, get_value_str) {
    const Config::Config config = {
        {"KEY1", "VALUE1"},
        {"KEY2", " \t\n\v\f\r"},
    };

    std::string value;

    EXPECT_EQ(Config::get_value(config, "KEY1", value), Config::FOUND);
    EXPECT_EQ(value, "VALUE1");

    EXPECT_EQ(Config::get_value(config, "KEY2", value), Config::FOUND);
    EXPECT_EQ(value, " \t\n\v\f\r"); // whitespace should not be removed

    EXPECT_EQ(Config::get_value(config, "KEY3", value), Config::NOT_FOUND);
}

TEST(Config, get_value_int) {
    const Config::Config config = {
        {"KEY1", "1"},
        {"KEY2", ""},
    };

    int value;

    EXPECT_EQ(Config::get_value(config, "KEY1", value), Config::FOUND);
    EXPECT_EQ(value, 1);

    EXPECT_EQ(Config::get_value(config, "KEY2", value), Config::ERROR);

    EXPECT_EQ(Config::get_value(config, "KEY3", value), Config::NOT_FOUND);
}

TEST(Config, get_value_bool) {
    const Config::Config config = {
        {"KEY1", "true"},
        {"KEY2", "false"},
        {"KEY3", ""},
    };

    bool value = false;

    EXPECT_EQ(Config::get_value(config, "KEY1", value), Config::FOUND);
    EXPECT_EQ(value, true);

    EXPECT_EQ(Config::get_value(config, "KEY2", value), Config::FOUND);
    EXPECT_EQ(value, false);

    EXPECT_EQ(Config::get_value(config, "KEY3", value), Config::ERROR);

    EXPECT_EQ(Config::get_value(config, "KEY4", value), Config::NOT_FOUND);
}

TEST(Config, get_from_map) {
    const std::unordered_map<std::string, std::string> keywords = {
        {"KEYWORD1", "VALUE1"},
    };

    const Config::Config config = {
        {"KEY1", "KEYWORD1"},
        {"KEY2", "KEYWORD2"},
    };

    std::string value;

    EXPECT_EQ(Config::get_from_map(config, "KEY1", keywords, value), Config::FOUND);
    EXPECT_EQ(value, "VALUE1");

    EXPECT_EQ(Config::get_from_map(config, "KEY2", keywords, value), Config::ERROR);

    EXPECT_EQ(Config::get_from_map(config, "KEY3", keywords, value), Config::NOT_FOUND);
}
