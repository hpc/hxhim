#include <gtest/gtest.h>

#include "generic_options.hpp"
#include "hxhim/hxhim.hpp"

typedef uint64_t Subject_t;
typedef uint64_t Predicate_t;
typedef double   Object_t;

TEST(hxhim, PutGet) {
    const Subject_t SUBJECT     = (((Subject_t) rand()) << 32) | rand();
    const Predicate_t PREDICATE = (((Predicate_t) rand()) << 32) | rand();
    const Object_t OBJECT       = (((Object_t) SUBJECT) * ((Object_t) SUBJECT)) / (((Object_t) PREDICATE) * ((Object_t) PREDICATE));

    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    // Add triple for putting
    EXPECT_EQ(hxhim::PutDouble(&hx,
                               (void *)&SUBJECT, sizeof(SUBJECT),
                               (void *)&PREDICATE, sizeof(PREDICATE),
                               (double *)&OBJECT),
              HXHIM_SUCCESS);

    // Flush all queued items
    hxhim::Results *put_results = hxhim::FlushPuts(&hx);
    ASSERT_NE(put_results, nullptr);

    // Make sure put succeeded
    EXPECT_EQ(put_results->size(), (std::size_t) 1);
    for(put_results->GoToHead(); put_results->Valid(); put_results->GoToNext()) {
        hxhim::Results::Result *res = put_results->Curr();
        ASSERT_NE(res, nullptr);

        EXPECT_EQ(res->status, HXHIM_SUCCESS);
        EXPECT_EQ(res->type, HXHIM_RESULT_PUT);
    }

    hxhim::Results::Destroy(put_results);

    // Add subject-predicate to get back
    EXPECT_EQ(hxhim::GetDouble(&hx,
                               (void *)&SUBJECT, sizeof(SUBJECT),
                               (void *)&PREDICATE, sizeof(PREDICATE)),
              HXHIM_SUCCESS);

    // Flush all queued items
    hxhim::Results *get_results = hxhim::FlushGets(&hx);
    ASSERT_NE(get_results, nullptr);

    // get the results and compare them with the original data
    EXPECT_EQ(get_results->size(), (std::size_t) 1);
    for(get_results->GoToHead(); get_results->Valid(); get_results->GoToNext()) {
        hxhim::Results::Result *res = get_results->Curr();
        ASSERT_NE(res, nullptr);

        ASSERT_EQ(res->status, HXHIM_SUCCESS);
        ASSERT_EQ(res->type, HXHIM_RESULT_GET);

        hxhim::Results::Get *get = static_cast<hxhim::Results::Get *>(get_results->Curr());

        Subject_t *subject = static_cast<Subject_t *>(get->subject->data());
        EXPECT_EQ(*subject, SUBJECT);

        Predicate_t *predicate = static_cast<Predicate_t *>(get->predicate->data());
        EXPECT_EQ(*predicate, PREDICATE);

        Object_t *object = static_cast<Object_t *>(get->object->data());
        ASSERT_NE(object, nullptr);
        if (std::is_same<float, Object_t>::value) {
            EXPECT_FLOAT_EQ(*object, OBJECT);
        }
        else if (std::is_same<double, Object_t>::value) {
            EXPECT_DOUBLE_EQ(*object, OBJECT);
        }
    }

    hxhim::Results::Destroy(get_results);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
