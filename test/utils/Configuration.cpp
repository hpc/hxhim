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

TEST(ConfigSequence, next_index) {
    ConfigSequence config_sequence;

    // starts at 0
    EXPECT_EQ(config_sequence.next_index(), 0);

    // incremental insertion
    std::vector<ConfigOnePair *> readers;
    const std::size_t COUNT = 10;
    for(std::size_t i = 0; i < COUNT; i++) {
        ConfigOnePair *reader = new ConfigOnePair("KEY VALUE1");
        readers.push_back(reader);
        EXPECT_EQ(config_sequence.add(reader), i + 1);
    }

    // non consequtive position
    readers.push_back(new ConfigOnePair("KEY VALUE2"));
    EXPECT_EQ(config_sequence.add(100, readers.back()), 101);

    // overwrite previously used position
    readers.push_back(new ConfigOnePair("KEY VALUE3"));
    EXPECT_EQ(config_sequence.add(COUNT, readers.back()), 101);

    // cleanup
    for(ConfigOnePair *reader : readers) {
        delete reader;
    }
}

TEST(ConfigSequence, usage) {
    ConfigSequence config_sequence;

    // add to index 2
    ConfigOnePair index2("KEY VALUE2");
    EXPECT_EQ(config_sequence.add(2, &index2), 3);

    // add to index 1 (preferred over index 2)
    ConfigOnePair index1("KEY VALUE1");
    EXPECT_EQ(config_sequence.add(1, &index1), 3);

    // read the configuration
    Config config;

    // only read up to index 1
    config_sequence.process(config);
    EXPECT_EQ(config.size(), 1);
    EXPECT_EQ(config.at("KEY"), "VALUE1");

    // add to index 0 (preferred over index 1)
    ConfigOnePair index0("KEY VALUE0");
    EXPECT_EQ(config_sequence.add(1, &index0), 3);

    // only read up to index 0
    config_sequence.process(config);
    EXPECT_EQ(config.size(), 1);
    EXPECT_EQ(config.at("KEY"), "VALUE0");
}
