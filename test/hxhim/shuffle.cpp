#include <gtest/gtest.h>

#include "generic_options.hpp"
#include "hxhim/accessors.hpp"
#include "hxhim/hxhim.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/private/cache.hpp"
#include "hxhim/private/shuffle.hpp"
#include "transport/Messages/Messages.hpp"
#include "utils/Blob.hpp"
#include "utils/memory.hpp"

TEST(hxhim, shuffle) {
    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);

    // pack at most 1 set of data into one packet
    ASSERT_EQ(hxhim_options_set_maximum_ops_per_send(&opts, 1), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    int rank = -1;
    ASSERT_EQ(hxhim::GetMPI(&hx, nullptr, &rank, nullptr), HXHIM_SUCCESS);
    ASSERT_NE(rank, -1);

    Blob sub1                 = ReferenceBlob((void *) "sub1",  4);
    Blob pred1                = ReferenceBlob((void *) "pred1", 5);
    hxhim_object_type_t type1 = hxhim_object_type_t::HXHIM_OBJECT_TYPE_BYTE;
    Blob obj1                 = ReferenceBlob((void *) "obj1",  4);
    hxhim::PutData put1(&hx, sub1, pred1, type1, obj1);

    Blob sub2                 = ReferenceBlob((void *) "sub2",  4);
    Blob pred2                = ReferenceBlob((void *) "pred2", 5);
    hxhim_object_type_t type2 = hxhim_object_type_t::HXHIM_OBJECT_TYPE_BYTE;
    Blob obj2                 = ReferenceBlob((void *) "obj2",  4);
    hxhim::PutData put2(&hx, sub2, pred2, type2, obj2);

    Transport::Request::BPut   local(2);
    Transport::Request::BPut **remote = alloc_array<Transport::Request::BPut *>(rank + 1);
    remote[rank] = &local;

    auto created = [&rank](Transport::Request::BPut **remote){
        int count = 0;
        for(int i = 0; i <= rank; i++) {
            count += (bool) remote[i];
        }
        return count;
    };

    // start empty
    EXPECT_EQ(local.count,     0);
    EXPECT_EQ(created(remote), 1);

    // move to the local rank buffer
    EXPECT_EQ(hxhim::shuffle::shuffle(&hx, &put1, remote), rank);
    EXPECT_EQ(local.count,     1);
    EXPECT_EQ(created(remote), 1);

    // local buffer is "full", so return NOSPACE
    EXPECT_EQ(hxhim::shuffle::shuffle(&hx, &put2, remote), hxhim::shuffle::NOSPACE);
    EXPECT_EQ(local.count,     1);
    EXPECT_EQ(created(remote), 1);

    // overwrite max count
    hx.p->max_ops_per_send = 2;

    // move to the local rank buffer
    EXPECT_EQ(hxhim::shuffle::shuffle(&hx, &put2, remote), rank);
    EXPECT_EQ(local.count,     2);
    EXPECT_EQ(created(remote), 1);

    dealloc_array(remote);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
