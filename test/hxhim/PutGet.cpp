#include <cstdlib>
#include <ctime>

#include <gtest/gtest.h>
#include <mpi.h>

#include "hxhim/hxhim.hpp"

typedef uint64_t Subject_t;
typedef uint64_t Predicate_t;
typedef uint64_t Object_t;

TEST(hxhim, PutGet) {
    srand(time(NULL));

    const Subject_t SUBJECT     = (((Subject_t) rand()) << 32) | rand();
    const Predicate_t PREDICATE = (((Predicate_t) rand()) << 32) | rand();
    const Object_t OBJECT       = SUBJECT ^ PREDICATE;

    hxhim_t hx;

    ASSERT_EQ(hxhim::Open(&hx, MPI_COMM_WORLD), HXHIM_SUCCESS);

    // Add triple for putting
    EXPECT_EQ(hxhim::Put(&hx,
                         (void *)&SUBJECT, sizeof(SUBJECT),
                         (void *)&PREDICATE, sizeof(PREDICATE),
                         (void *)&OBJECT, sizeof(OBJECT)),
              HXHIM_SUCCESS);

    // Flush all queued items
    hxhim::Results *put_results = hxhim::Flush(&hx);
    ASSERT_NE(put_results, nullptr);
    delete put_results;

    // Add subject-predicate to get back
    EXPECT_EQ(hxhim::Get(&hx,
                         (void *)&SUBJECT, sizeof(SUBJECT),
                         (void *)&PREDICATE, sizeof(PREDICATE)),
              HXHIM_SUCCESS);

    // Flush all queued items
    hxhim::Results *get_results = hxhim::Flush(&hx);
    ASSERT_NE(get_results, nullptr);

    // // go to first range server and make sure that this range server is valid
    // ASSERT_EQ(get_results->MoveToFirstRS(), HXHIM_SUCCESS);
    // ASSERT_EQ(get_results->ValidRS(), HXHIM_SUCCESS);

    // // go to first key value pair and make sure that the key value pairs can be obtained
    // ASSERT_EQ(get_results->MoveToFirstSPO(), HXHIM_SUCCESS);
    // ASSERT_EQ(get_results->GetError(), HXHIM_SUCCESS);

    // // get the key value pair back
    // ASSERT_EQ(get_results->ValidSPO(), HXHIM_SUCCESS);
    // Object_t *object; std::size_t object_len;
    // ASSERT_EQ(get_results->GetSPO(nullptr, nullptr, nullptr, nullptr, (void **) &object, &object_len), HXHIM_SUCCESS);
    // EXPECT_EQ(*object, OBJECT);
    // EXPECT_EQ(object_len, sizeof(OBJECT));

    // // go to the next key value pair
    // EXPECT_NE(get_results->NextSPO(), HXHIM_SUCCESS);

    // // go to the next range server
    // EXPECT_NE(get_results->NextRS(), HXHIM_SUCCESS);

    // // go to the next response
    // EXPECT_EQ(get_results->Next(), nullptr);

    delete get_results;

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
}
