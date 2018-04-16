#include <sstream>

#include <gtest/gtest.h>

#include "ConfigReader.hpp"

// function for reading configuration from a stringstream
static bool stringstream_parser(ConfigReader::Config_t &config, const std::string &src) {
    std::stringstream s(src);
    std::string key, value;
    if (!(s >> key >> value)) {
        return false;
    }

    config[key] = value;
    return true;
}

TEST(ConfigReader, next_index) {
    ConfigReader configreader;

    // starts at 0
    EXPECT_EQ(configreader.next_index(), 0);

    // incremental insertion
    const std::size_t COUNT = 10;
    for(std::size_t i = 0; i < COUNT; i++) {
        EXPECT_EQ(configreader.add(stringstream_parser, "KEY VALUE1"), i + 1);
    }

    // non consequtive position
    EXPECT_EQ(configreader.add(100, stringstream_parser, "KEY VALUE2"), 101);

    // overwrite previously used position
    EXPECT_EQ(configreader.add(COUNT, stringstream_parser, "KEY VALUE3"), 101);
}

TEST(ConfigReader, usage) {
    ConfigReader configreader;

    // add to index 2
    EXPECT_EQ(configreader.add(2, stringstream_parser, "KEY VALUE2"), 3);

    // add to index 1 (preferred over index 2)
    EXPECT_EQ(configreader.add(1, stringstream_parser, "KEY VALUE1"), 3);

    // read the configuration
    ConfigReader::Config_t config;

    // only read up to index 1
    EXPECT_EQ(configreader.read(config), true);
    EXPECT_EQ(config.size(), 1);
    EXPECT_EQ(config.at("KEY"), "VALUE1");

    // add to index 0 (preferred over index 1)
    EXPECT_EQ(configreader.add(1, stringstream_parser, "KEY VALUE0"), 3);

    // only read up to index 0
    EXPECT_EQ(configreader.read(config), true);
    EXPECT_EQ(config.size(), 1);
    EXPECT_EQ(config.at("KEY"), "VALUE0");
}
