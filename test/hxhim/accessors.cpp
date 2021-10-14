#include <cmath>

#include <gtest/gtest.h>

#include "generic_options.hpp"
#include "hxhim/hxhim.hpp"

TEST(GetRangeServerCount, equal) {
    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);
    EXPECT_EQ(hxhim_options_set_client_ratio(&opts, 1), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_set_server_ratio(&opts, 1), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    int size = -1;
    EXPECT_EQ(hxhim::GetMPI(&hx, nullptr, nullptr, &size), HXHIM_SUCCESS);

    std::size_t count = 0;
    EXPECT_EQ(hxhim::GetRangeServerCount(&hx, &count), HXHIM_SUCCESS);
    EXPECT_EQ(count, size);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}

TEST(GetRangeServerCount, more_clients) {
    /**
     * Client : Server = 5 : 3
     * MPI Rank: |  0  |  1  |  2  |  3  |  4  |  5  |  6  |  7  |  8  |
     * Client:   |  0  |  1  |  2  |  3  |  4  |  5  |  6  |  7  |  8  |
     * RS/DS:    |  0  |  1  |  2  |     |     |  3  |  4  |  5  |     |
     */

    const std::size_t CLIENT_RATIO = 5;
    const std::size_t SERVER_RATIO = 3;

    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);
    EXPECT_EQ(hxhim_options_set_client_ratio(&opts, CLIENT_RATIO), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_set_server_ratio(&opts, SERVER_RATIO), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    int size = -1;
    EXPECT_EQ(hxhim::GetMPI(&hx, nullptr, nullptr, &size), HXHIM_SUCCESS);

    std::size_t count = 0;
    EXPECT_EQ(hxhim::GetRangeServerCount(&hx, &count), HXHIM_SUCCESS);
    EXPECT_EQ(count, ((size / CLIENT_RATIO) * SERVER_RATIO + std::min(size % CLIENT_RATIO, SERVER_RATIO)));

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}

TEST(GetRangeServerCount, more_servers) {
    /**
     * Client : Server = 3 : 5
     * MPI Rank: |  0  |  1  |  2  |  3  |  4  |  5  |  6  |  7  |  8  |
     * Client:   |  0  |  1  |  2  |  3  |  4  |  5  |  6  |  7  |  8  |
     * RS/DS:    |  0  |  1  |  2  |  3  |  4  |  5  |  6  |  7  |  8  |
     */

    const std::size_t CLIENT_RATIO = 3;
    const std::size_t SERVER_RATIO = 5;

    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);
    EXPECT_EQ(hxhim_options_set_client_ratio(&opts, CLIENT_RATIO), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_set_server_ratio(&opts, SERVER_RATIO), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    int size = -1;
    EXPECT_EQ(hxhim::GetMPI(&hx, nullptr, nullptr, &size), HXHIM_SUCCESS);

    std::size_t count = 0;
    EXPECT_EQ(hxhim::GetRangeServerCount(&hx, &count), HXHIM_SUCCESS);
    EXPECT_EQ(count, size);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
