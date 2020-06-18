#include <unordered_map>

#include <mpi.h>
#include <gtest/gtest.h>

#include "generic_options.hpp"
#include "hxhim/cache.hpp"
#include "hxhim/hxhim.hpp"
#include "hxhim/options.hpp"
#include "hxhim/shuffle.hpp"
#include "transport/Messages/Messages.hpp"
#include "utils/Blob.hpp"
#include "utils/memory.hpp"

TEST(hxhim, shuffle) {
    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);

    // overwrite default hash
    int dst;
    ASSERT_EQ(hxhim_options_set_hash_function(&opts,
                                              "test has",
                                              [](hxhim_t *, void *, const size_t,
                                                 void *, const size_t, void *args) {
                                                  return * (int *) args;
                                              },
                                              &dst), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    // have to construct because Transport::Message::B* takes ownership and calls destruct
    ReferenceBlob *sub1  = construct<ReferenceBlob>((void *) "sub1",  4);
    ReferenceBlob *pred1 = construct<ReferenceBlob>((void *) "pred1", 5);
    hxhim_type_t   type1 = HXHIM_BYTE_TYPE;
    ReferenceBlob *obj1  = construct<ReferenceBlob>((void *) "obj1",  4);
    hxhim::PutData put1;
    put1.subject         = sub1;
    put1.predicate       = pred1;
    put1.object_type     = type1;
    put1.object          = obj1;

    ReferenceBlob *sub2  = construct<ReferenceBlob>((void *) "sub2",  4);
    ReferenceBlob *pred2 = construct<ReferenceBlob>((void *) "pred2", 5);
    hxhim_type_t   type2 = HXHIM_BYTE_TYPE;
    ReferenceBlob *obj2  = construct<ReferenceBlob>((void *) "obj2",  4);
    hxhim::PutData put2;
    put2.subject         = sub2;
    put2.predicate       = pred2;
    put2.object_type     = type2;
    put2.object          = obj2;

    Transport::Request::BPut local(2);
    std::unordered_map<int, Transport::Request::BPut *> remote; // not used, but reference is needed

    // start empty
    EXPECT_EQ(local.count,   0);
    EXPECT_EQ(remote.size(), 0);

    // bad destination
    dst = -1;
    EXPECT_EQ(hxhim::shuffle::shuffle(&hx, 2, &put1, &local, remote), hxhim::shuffle::ERROR);
    EXPECT_EQ(local.count,   0);
    EXPECT_EQ(remote.size(), 0);

    // "send" to the local rank
    ASSERT_EQ(MPI_Comm_rank(MPI_COMM_WORLD, &dst), MPI_SUCCESS);

    // move into local buffer
    EXPECT_EQ(hxhim::shuffle::shuffle(&hx, 2, &put1, &local, remote), dst);
    EXPECT_EQ(local.count,   1);
    EXPECT_EQ(remote.size(), 0);

    // local buffer is "full", so return NOSPACE
    EXPECT_EQ(hxhim::shuffle::shuffle(&hx, 1, &put2, &local, remote), hxhim::shuffle::NOSPACE);
    EXPECT_EQ(local.count,   1);
    EXPECT_EQ(remote.size(), 0);

    // move into local buffer
    EXPECT_EQ(hxhim::shuffle::shuffle(&hx, 2, &put2, &local, remote), dst);
    EXPECT_EQ(local.count,   2);
    EXPECT_EQ(remote.size(), 0);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
