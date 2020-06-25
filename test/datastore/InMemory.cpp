#include <cstring>

#include <gtest/gtest.h>

#ifndef HXHIM_DATASTORE_GTEST
#define HXHIM_DATASTORE_GTEST
#endif

#include "datastore/InMemory.hpp"
#include "hxhim/triplestore.hpp"
#include "utils/memory.hpp"

static const char *subs[]         = {"sub1",          "sub2"};
static const char *preds[]        = {"pred1",         "pred2"};
static const hxhim_type_t types[] = {HXHIM_BYTE_TYPE, HXHIM_BYTE_TYPE};
static const char *objs[]         = {"obj1",          "obj2"};
static const std::size_t count    = sizeof(types) / sizeof(types[0]);

class InMemoryTest : public hxhim::datastore::InMemory {
    public:
        InMemoryTest()
            : hxhim::datastore::InMemory(nullptr, 0, nullptr, "InMemory test")
        {}

        std::map<std::string, std::string> const & data() const {
            return db;
        }
};

// create a test InMemory datastore and insert some triples
static InMemoryTest *setup() {
    InMemoryTest *ds = construct<InMemoryTest>();

    Transport::Request::BPut req(count);

    for(std::size_t i = 0; i < count; i++) {
        req.subjects[i]     = construct<ReferenceBlob>((void *) subs[i],  strlen(subs[i]));
        req.predicates[i]   = construct<ReferenceBlob>((void *) preds[i], strlen(preds[i]));
        req.object_types[i] = types[i];
        req.objects[i]      = construct<ReferenceBlob>((void *) objs[i],  strlen(objs[i]));
        req.count++;
    }

    Transport::Response::BPut * res = ds->operate(&req);
    destruct(res);

    return ds;
}

TEST(InMemory, BPut) {
    InMemoryTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    std::map<std::string, std::string> const &db = ds->data();
    EXPECT_EQ(db.size(), 2U);

    for(std::size_t i = 0; i < count; i++) {
        void * key = nullptr;
        std::size_t key_len = 0;
        EXPECT_EQ(sp_to_key(subs[i], strlen(subs[i]),
                            preds[i], strlen(preds[i]),
                            &key, &key_len), HXHIM_SUCCESS);

        EXPECT_EQ(db.find(std::string((char *) key, key_len)) != db.end(), true);

        dealloc(key);
    }

    destruct(ds);
}

TEST(InMemory, BGet) {
    InMemoryTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    Transport::Request::BGet req(count + 1);
    for(std::size_t i = 0; i < count; i++) {
        req.subjects[i]     = construct<ReferenceBlob>((void *) subs[i],  strlen(subs[i]));
        req.predicates[i]   = construct<ReferenceBlob>((void *) preds[i], strlen(preds[i]));
        req.object_types[i] = types[i];
        req.count++;
    }

    // non-existant subject-predicate pair
    req.subjects[count]     = construct<ReferenceBlob>((void *) "sub3",  4);
    req.predicates[count]   = construct<ReferenceBlob>((void *) "pred3", 5);
    req.object_types[count] = HXHIM_BYTE_TYPE;
    req.count++;

    Transport::Response::BGet *res = ds->operate(&req);
    ASSERT_NE(res, nullptr);
    EXPECT_EQ(res->count, count + 1);
    for(std::size_t i = 0; i < count; i++) {
        EXPECT_EQ(res->statuses[i], HXHIM_SUCCESS);
        ASSERT_NE(res->objects[i], nullptr);
        EXPECT_EQ(res->objects[i]->size(), strlen(objs[i]));
        EXPECT_EQ(std::memcmp(objs[i], res->objects[i]->data(), res->objects[i]->size()), 0);
    }

    EXPECT_EQ(res->statuses[count], HXHIM_ERROR);
    EXPECT_EQ(res->objects[count], nullptr);

    destruct(res);
    destruct(ds);
}

TEST(InMemory, BDelete) {
    InMemoryTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    std::map<std::string, std::string> const &db = ds->data();
    EXPECT_EQ(db.size(), 2U);

    Transport::Request::BDelete req(count + 1);
    for(std::size_t i = 0; i < count; i++) {
        req.subjects[i]     = construct<ReferenceBlob>((void *) subs[i],  strlen(subs[i]));
        req.predicates[i]   = construct<ReferenceBlob>((void *) preds[i], strlen(preds[i]));
        req.count++;
    }

    // non-existant subject-predicate pair
    req.subjects[count]     = construct<ReferenceBlob>((void *) "sub3",  4);
    req.predicates[count]   = construct<ReferenceBlob>((void *) "pred3", 5);
    req.count++;

    Transport::Response::BDelete *res = ds->operate(&req);
    ASSERT_NE(res, nullptr);
    EXPECT_EQ(res->count, count + 1);
    for(std::size_t i = 0; i < count; i++) {
        EXPECT_EQ(res->statuses[i], HXHIM_SUCCESS);
    }

    EXPECT_EQ(res->statuses[count], HXHIM_ERROR);

    destruct(res);

    // get directly from internal data
    for(std::size_t i = 0; i < count; i++) {
        void * key = nullptr;
        std::size_t key_len = 0;
        EXPECT_EQ(sp_to_key(subs[i], strlen(subs[i]),
                            preds[i], strlen(preds[i]),
                            &key, &key_len), HXHIM_SUCCESS);

        EXPECT_EQ(db.find(std::string((char *) key, key_len)) == db.end(), true);
        dealloc(key);
    }

    destruct(ds);
}
