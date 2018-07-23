#include <cstring>

#include <gtest/gtest.h>

#include "hxhim/Results.hpp"

static const int ERROR = HXHIM_SUCCESS;
static const int DATABASE = 1;
static const char *SUBJECT = "subject";
static const std::size_t SUBJECT_LEN = strlen(SUBJECT);
static const char *PREDICATE = "predicate";
static const std::size_t PREDICATE_LEN = strlen(PREDICATE);
static const char *OBJECT = "object";
static const std::size_t OBJECT_LEN = strlen(OBJECT);

/** @description Convenience class for GET results */
class TestGet : public hxhim::Results::Get {
    public:
        TestGet(const int err, const int db,
                void *subject, std::size_t subject_len,
                void *predicate, std::size_t predicate_len,
                void *object, std::size_t object_len)
            : Get(err, db),
              sub(subject), sub_len(subject_len),
              pred(predicate), pred_len(predicate_len),
              obj(object), obj_len(object_len)
        {}

        ~TestGet() {
            ::operator delete(sub);
            ::operator delete(pred);
            ::operator delete(obj);
        }

        int GetSubject(void **subject, std::size_t *subject_len) const {
            if (subject) *subject = sub;
            if (subject_len) *subject_len = sub_len;
            return HXHIM_SUCCESS;
        }

        int GetPredicate(void **predicate, std::size_t *predicate_len) const {
            if (predicate) *predicate = sub;
            if (predicate_len) *predicate_len = pred_len;
            return HXHIM_SUCCESS;
        }

        int GetObject(void **object, std::size_t *object_len) const {
            if (object) *object = obj;
            if (object_len) *object_len = obj_len;
            return HXHIM_SUCCESS;
        }

    private:
        void *sub;
        std::size_t sub_len;
        void *pred;
        std::size_t pred_len;
        void *obj;
        std::size_t obj_len;
};

TEST(hxhim, Results) {
    hxhim::Results results(HXHIM_SPO_BYTE_TYPE, HXHIM_SPO_BYTE_TYPE, HXHIM_SPO_BYTE_TYPE);

    // nothing works yet since there is no data
    EXPECT_EQ(results.GoToHead(), nullptr);
    EXPECT_EQ(results.GoToNext(), nullptr);
    EXPECT_EQ(results.Valid(), false);

    // add some data
    hxhim::Results::Result *put = results.Add(new hxhim::Results::Put(ERROR, DATABASE));
    EXPECT_NE(put, nullptr);

    // pretend this data came from a backend
    void *subject = ::operator new(SUBJECT_LEN);
    memcpy(subject, SUBJECT, SUBJECT_LEN);
    void *predicate = ::operator new(PREDICATE_LEN);
    memcpy(predicate, PREDICATE, PREDICATE_LEN);
    void *object = ::operator new(OBJECT_LEN);
    memcpy(object, OBJECT, OBJECT_LEN);

    hxhim::Results::Result *get = results.Add(new TestGet(ERROR, DATABASE, subject, SUBJECT_LEN, predicate, PREDICATE_LEN, object, OBJECT_LEN));
    EXPECT_NE(get, nullptr);
    hxhim::Results::Result *del = results.Add(new hxhim::Results::Delete(ERROR, DATABASE));
    EXPECT_NE(del, nullptr);

    EXPECT_EQ(results.Valid(), false);  // still not valid because current result has not been set yet
    EXPECT_EQ(results.GoToHead(), put);
    EXPECT_EQ(results.Valid(), true);   // valid now because current result is pointing to the head of the list
    EXPECT_EQ(results.GoToNext(), get);
    EXPECT_EQ(results.GoToNext(), del);
}

TEST(hxhim, Results_Append_Empty) {
    hxhim::Results results(HXHIM_SPO_BYTE_TYPE, HXHIM_SPO_BYTE_TYPE, HXHIM_SPO_BYTE_TYPE);

    // add some data
    hxhim::Results::Result *put = results.Add(new hxhim::Results::Put(ERROR, DATABASE));
    EXPECT_NE(put, nullptr);

    // pretend this data came from a backend
    void *subject = ::operator new(SUBJECT_LEN);
    memcpy(subject, SUBJECT, SUBJECT_LEN);
    void *predicate = ::operator new(PREDICATE_LEN);
    memcpy(predicate, PREDICATE, PREDICATE_LEN);
    void *object = ::operator new(OBJECT_LEN);
    memcpy(object, OBJECT, OBJECT_LEN);

    hxhim::Results::Result *get = results.Add(new TestGet(ERROR, DATABASE, subject, SUBJECT_LEN, predicate, PREDICATE_LEN, object, OBJECT_LEN));
    EXPECT_NE(get, nullptr);
    hxhim::Results::Result *del = results.Add(new hxhim::Results::Delete(ERROR, DATABASE));
    EXPECT_NE(del, nullptr);

    // append empty set of results
    hxhim::Results empty(HXHIM_SPO_BYTE_TYPE, HXHIM_SPO_BYTE_TYPE, HXHIM_SPO_BYTE_TYPE);
    results.Append(&empty);

    EXPECT_EQ(results.GoToHead(), put); // first result is PUT
    EXPECT_EQ(results.GoToNext(), get); // next result is GET
    EXPECT_EQ(results.GoToNext(), del); // next result is DEL
}

TEST(hxhim, Empty_Append_Results) {
    // empty set of results
    hxhim::Results results(HXHIM_SPO_BYTE_TYPE, HXHIM_SPO_BYTE_TYPE, HXHIM_SPO_BYTE_TYPE);

    hxhim::Results empty(HXHIM_SPO_BYTE_TYPE, HXHIM_SPO_BYTE_TYPE, HXHIM_SPO_BYTE_TYPE);

    // add some data to the non-empty results
    hxhim::Results::Result *put = results.Add(new hxhim::Results::Put(ERROR, DATABASE));
    EXPECT_NE(put, nullptr);

    // pretend this data came from a backend
    void *subject = ::operator new(SUBJECT_LEN);
    memcpy(subject, SUBJECT, SUBJECT_LEN);
    void *predicate = ::operator new(PREDICATE_LEN);
    memcpy(predicate, PREDICATE, PREDICATE_LEN);
    void *object = ::operator new(OBJECT_LEN);
    memcpy(object, OBJECT, OBJECT_LEN);

    hxhim::Results::Result *get = results.Add(new TestGet(ERROR, DATABASE, subject, SUBJECT_LEN, predicate, PREDICATE_LEN, object, OBJECT_LEN));
    EXPECT_NE(get, nullptr);
    hxhim::Results::Result *del = results.Add(new hxhim::Results::Delete(ERROR, DATABASE));
    EXPECT_NE(del, nullptr);

    empty.Append(&results);

    EXPECT_EQ(empty.GoToHead(), put); // first result is PUT
    EXPECT_EQ(empty.GoToNext(), get); // next result is GET
    EXPECT_EQ(empty.GoToNext(), del); // next result is DEL

    // appending moves the contents of the result list
    EXPECT_EQ(results.GoToHead(), nullptr);
}
