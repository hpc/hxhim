#include <gtest/gtest.h>

#include "generic_options.hpp"
#include "hxhim/hxhim.hpp"

static const std::size_t DATASTORES = 4;
static const std::size_t SMALLER = 3;
static const std::size_t LARGER = 5;

TEST(GetDatastoreCount, equal) {
    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);
    EXPECT_EQ(hxhim_options_set_client_ratio(&opts, 1), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_set_server_ratio(&opts, 1), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_set_datastores_per_range_server(&opts, DATASTORES), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    int size = -1;
    EXPECT_EQ(hxhim::GetMPI(&hx, nullptr, nullptr, &size), HXHIM_SUCCESS);

    std::size_t count = 0;
    EXPECT_EQ(hxhim::GetDatastoreCount(&hx, &count), HXHIM_SUCCESS);
    EXPECT_EQ(count, DATASTORES * size);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}

TEST(GetDatastoreCount, more_clients) {
    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);
    EXPECT_EQ(hxhim_options_set_client_ratio(&opts, LARGER), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_set_server_ratio(&opts, SMALLER), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_set_datastores_per_range_server(&opts, DATASTORES), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    int size = -1;
    EXPECT_EQ(hxhim::GetMPI(&hx, nullptr, nullptr, &size), HXHIM_SUCCESS);

    std::size_t count = 0;
    EXPECT_EQ(hxhim::GetDatastoreCount(&hx, &count), HXHIM_SUCCESS);
    EXPECT_EQ(count, DATASTORES * ((size / LARGER) * SMALLER + std::min(size % LARGER, SMALLER)));

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}

TEST(GetDatastoreCount, more_servers) {
    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);
    EXPECT_EQ(hxhim_options_set_client_ratio(&opts, SMALLER), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_set_server_ratio(&opts, LARGER), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_set_datastores_per_range_server(&opts, DATASTORES), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    int size = -1;
    EXPECT_EQ(hxhim::GetMPI(&hx, nullptr, nullptr, &size), HXHIM_SUCCESS);

    std::size_t count = 0;
    EXPECT_EQ(hxhim::GetDatastoreCount(&hx, &count), HXHIM_SUCCESS);
    EXPECT_EQ(count, DATASTORES * size);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
