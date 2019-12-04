#include <cstdlib>
#include <ctime>
#include <type_traits>

#include <gtest/gtest.h>
#include <mpi.h>

#include "generic_options.hpp"
#include "hxhim/hxhim.hpp"

TEST(transport, MPI) {
    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);
    ASSERT_EQ(hxhim_options_set_transport_mpi(&opts, 10), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}

#define TEST_THALLIUM_TRANSPORT(plugin, protocol)                                                      \
    TEST(transport, thallium_ ##plugin ##_ ##protocol) {                                               \
        hxhim_options_t opts;                                                                          \
        ASSERT_EQ(fill_options(&opts), true);                                                          \
        ASSERT_EQ(hxhim_options_set_transport_thallium(&opts, #plugin "+" #protocol), HXHIM_SUCCESS);  \
                                                                                                       \
        hxhim_t hx;                                                                                    \
        ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);                                             \
                                                                                                       \
        EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);                                                   \
        EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);                                        \
    }                                                                                                  \

TEST_THALLIUM_TRANSPORT(na, sm)

// #ifdef BMI
// TEST_THALLIUM_TRANSPORT(bmi, tcp)
// #endif

// #ifdef CCI
TEST_THALLIUM_TRANSPORT(cci, tcp)
TEST_THALLIUM_TRANSPORT(cci, sm)
// #endif
