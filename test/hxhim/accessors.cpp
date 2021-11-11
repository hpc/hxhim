#include <cmath>

#include <gtest/gtest.h>

#include "generic_options.hpp"
#include "hxhim/hxhim.hpp"

TEST(GetRangeServerCount, equal) {
    hxhim_t hx;
    ASSERT_EQ(hxhim::Init(&hx, MPI_COMM_WORLD), HXHIM_SUCCESS);
    ASSERT_EQ(fill_options(&hx), true);
    EXPECT_EQ(hxhim_set_client_ratio(&hx, 1), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_set_server_ratio(&hx, 1), HXHIM_SUCCESS);

    ASSERT_EQ(hxhim::Open(&hx), HXHIM_SUCCESS);

    int size = -1;
    EXPECT_EQ(hxhim::GetMPI(&hx, nullptr, nullptr, &size), HXHIM_SUCCESS);

    std::size_t count = 0;
    EXPECT_EQ(hxhim::GetRangeServerCount(&hx, &count), HXHIM_SUCCESS);
    EXPECT_EQ(count, size);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
}

TEST(GetRangeServerCount, more_clients) {
    /**
     * Client : Server = 5 : 3
     * Datastores per server: 3
     * MPI Rank: |   0   |   1   |   2   |  3  |  4  |    5    |    6     |    7     |  8  |
     * Client:   |   0   |   1   |   2   |  3  |  4  |    5    |    6     |    7     |  8  |
     * RS:       |   0   |   1   |   2   |     |     |    3    |    4     |    5     |     |
     : DS:       | 0,1,2 | 3,4,5 | 6,7,8 |     |     | 9,10,11 | 12,13,14 | 15,16,17 |     |
     */

    const std::size_t CLIENT_RATIO = 5;
    const std::size_t SERVER_RATIO = 3;

    hxhim_t hx;
    ASSERT_EQ(hxhim::Init(&hx, MPI_COMM_WORLD), HXHIM_SUCCESS);
    ASSERT_EQ(fill_options(&hx), true);
    EXPECT_EQ(hxhim_set_client_ratio(&hx, CLIENT_RATIO), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_set_server_ratio(&hx, SERVER_RATIO), HXHIM_SUCCESS);

    ASSERT_EQ(hxhim::Open(&hx), HXHIM_SUCCESS);

    int size = -1;
    EXPECT_EQ(hxhim::GetMPI(&hx, nullptr, nullptr, &size), HXHIM_SUCCESS);

    std::size_t count = 0;
    EXPECT_EQ(hxhim::GetRangeServerCount(&hx, &count), HXHIM_SUCCESS);
    EXPECT_EQ(count, ((size / CLIENT_RATIO) * SERVER_RATIO + std::min(size % CLIENT_RATIO, SERVER_RATIO)));

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
}

TEST(GetRangeServerCount, more_servers) {
    /**
     * Client : Server = 3 : 5
     * Datastores per server: 3
     * MPI Rank: |   0   |   1   |   2   |    3    |    4     |    5     |    6     |    7     |    8     |
     * Client:   |   0   |   1   |   2   |    3    |    4     |    5     |    6     |    7     |    8     |
     * RS:       |   0   |   1   |   2   |    3    |    4     |    5     |    6     |    7     |    8     |
     * DS:       | 0,1,2 | 3,4,5 | 6,7,8 | 9,10,11 | 12,13,14 | 15,16,17 | 18,19,20 | 21,22,23 | 24,25,26 |
     */

    const std::size_t CLIENT_RATIO = 3;
    const std::size_t SERVER_RATIO = 5;

    hxhim_t hx;
    ASSERT_EQ(hxhim::Init(&hx, MPI_COMM_WORLD), HXHIM_SUCCESS);
    ASSERT_EQ(fill_options(&hx), true);
    EXPECT_EQ(hxhim_set_client_ratio(&hx, CLIENT_RATIO), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_set_server_ratio(&hx, SERVER_RATIO), HXHIM_SUCCESS);

    ASSERT_EQ(hxhim::Open(&hx), HXHIM_SUCCESS);

    int size = -1;
    EXPECT_EQ(hxhim::GetMPI(&hx, nullptr, nullptr, &size), HXHIM_SUCCESS);

    std::size_t count = 0;
    EXPECT_EQ(hxhim::GetRangeServerCount(&hx, &count), HXHIM_SUCCESS);
    EXPECT_EQ(count, size);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
}
