#include <gtest/gtest.h>

#include "utils/is_range_server.hpp"

TEST(util, is_range_server) {
    // bad is_range_server inputs
    {
        EXPECT_EQ(is_range_server(-1, 0, 1), -1);
        EXPECT_EQ(is_range_server( 0, 0, 1), -1);
        EXPECT_EQ(is_range_server( 0, 1, 0), -1);
    }

    // good is_range_server inputs
    {
        // all clients are also servers
        for(std::size_t i = 1; i < 5; i++) {
            for(std::size_t j = 0; j < i; j++) {
                EXPECT_EQ(is_range_server(j, i, i), 1);
            }
        }

        // more servers than clients
        for(std::size_t i = 0; i < 10; i++) {
            EXPECT_EQ(is_range_server(i, 1, 2), 1);
        }

        // every other client is a server
        // rank:    0 1 2 3 4 5 6 7 8 9
        // clients  T T T T T T T T T T
        // servers: T F T F T F T F T F
        for(std::size_t i = 0; i < 10; i++) {
            EXPECT_EQ(is_range_server(i, 2, 1), ! (bool) (i & 1));
        }

        // 3 servers for every 5 clients
        // rank:    0 1 2 3 4 5 6 7 8 9
        // clients: T T T T T T T T T T
        // servers: T T T F F T T T F F
        for(std::size_t i = 0; i < 2; i++) {
            for(std::size_t j = 0; j < 3; j++) {
                EXPECT_EQ(is_range_server(j + (i * 5), 5, 3), 1);
            }
            for(std::size_t j = 3; j < 5; j++) {
                EXPECT_EQ(is_range_server(j + (i * 5), 5, 3), 0);
            }
        }
    }
}
