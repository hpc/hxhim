#include <cstdlib>

#include <gtest/gtest.h>
#include <mpi.h>

#include "generic_options.hpp"
#include "hxhim/hxhim.hpp"

TEST(transport, MPI) {
    hxhim_t hx;
    ASSERT_EQ(hxhim::Init(&hx, MPI_COMM_WORLD), HXHIM_SUCCESS);
    ASSERT_EQ(fill_options(&hx), true);
    ASSERT_EQ(hxhim_set_transport_mpi(&hx, 10), HXHIM_SUCCESS);

    ASSERT_EQ(hxhim::Open(&hx), HXHIM_SUCCESS);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
}

#ifdef HXHIM_HAVE_THALLIUM
#define TEST_THALLIUM_TRANSPORT(plugin, protocol)                                                     \
    TEST(transport, thallium_ ##plugin ##_ ##protocol) {                                              \
        hxhim_t hx;                                                                                   \
        ASSERT_EQ(hxhim::Init(&hx, MPI_COMM_WORLD), HXHIM_SUCCESS);                                   \
        ASSERT_EQ(fill_options(&hx), true);                                                           \
        ASSERT_EQ(hxhim_set_transport_thallium(&hx, #plugin "+" #protocol), HXHIM_SUCCESS);           \
                                                                                                      \
        ASSERT_EQ(hxhim::Open(&hx), HXHIM_SUCCESS);                                                   \
                                                                                                      \
        EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);                                                  \
    }                                                                                                 \

TEST_THALLIUM_TRANSPORT(na, sm)

// #ifdef OFI
TEST_THALLIUM_TRANSPORT(ofi, tcp)
// #endif

// #ifdef BMI
// TEST_THALLIUM_TRANSPORT(bmi, tcp)
// #endif

// #ifdef CCI
// TEST_THALLIUM_TRANSPORT(cci, tcp)
// TEST_THALLIUM_TRANSPORT(cci, sm)
// #endif
#endif
