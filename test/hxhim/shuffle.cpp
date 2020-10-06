#include <gtest/gtest.h>

#include "generic_options.hpp"
#include "hxhim/hxhim.hpp"
#include "hxhim/private/cache.hpp"
#include "hxhim/private/shuffle.hpp"
#include "transport/Messages/Messages.hpp"
#include "utils/Blob.hpp"
#include "utils/memory.hpp"

TEST(hxhim, shuffle) {
    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    Blob sub1                 = ReferenceBlob((void *) "sub1",  4);
    Blob pred1                = ReferenceBlob((void *) "pred1", 5);
    hxhim_object_type_t type1 = hxhim_object_type_t::HXHIM_OBJECT_TYPE_BYTE;
    Blob obj1                 = ReferenceBlob((void *) "obj1",  4);
    hxhim::PutData put1(&hx, sub1, pred1, type1, obj1);
    put1.ds_rank = 0;

    Blob sub2                 = ReferenceBlob((void *) "sub2",  4);
    Blob pred2                = ReferenceBlob((void *) "pred2", 5);
    hxhim_object_type_t type2 = hxhim_object_type_t::HXHIM_OBJECT_TYPE_BYTE;
    Blob obj2                 = ReferenceBlob((void *) "obj2",  4);
    hxhim::PutData put2(&hx, sub2, pred2, type2, obj2);
    put2.ds_rank = 0;

    Transport::Request::BPut   local(2);
    Transport::Request::BPut  *request0 = &local;
    Transport::Request::BPut **requests = &request0;

    // start empty
    EXPECT_EQ(local.count, 0);

    // shuffle the first packet
    EXPECT_EQ(hxhim::shuffle::shuffle(&put1, 1, requests), 0);
    EXPECT_EQ(local.count, 1);

    // shuffle the second packet, but the
    // buffer is full, so return NOSPACE
    EXPECT_EQ(hxhim::shuffle::shuffle(&put2, 1, requests), hxhim::shuffle::NOSPACE);
    EXPECT_EQ(local.count, 1);

    // shuffle the second packet, but
    // this time there is enough space
    EXPECT_EQ(hxhim::shuffle::shuffle(&put2, 2, requests), 0);
    EXPECT_EQ(local.count, 2);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
