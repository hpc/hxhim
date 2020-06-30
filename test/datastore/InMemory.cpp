#include <cstring>

#include <gtest/gtest.h>

#include "datastore/InMemory.hpp"
#include "utils/memory.hpp"
#include "utils/triplestore.hpp"

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

        std::map<std::string, std::string> const &data() const {
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
        ReferenceBlob sub((void *) subs[i], strlen(subs[i]));
        ReferenceBlob pred((void *) preds[i], strlen(preds[i]));

        void *key = nullptr;
        std::size_t key_len = 0;
        EXPECT_EQ(sp_to_key(&sub, &pred,
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

TEST(InMemory, BGetOp) {
    InMemoryTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    for(int op = HXHIM_GET_EQ; op < HXHIM_GET_INVALID; op++) {
        Transport::Request::BGetOp req(1);
        req.subjects[0]     = construct<ReferenceBlob>((void *) subs[0],  strlen(subs[0]));
        req.predicates[0]   = construct<ReferenceBlob>((void *) preds[0], strlen(preds[0]));
        req.object_types[0] = types[0];
        req.num_recs[0]     = count + 1; // get too many records
        req.ops[0]          = static_cast<hxhim_get_op_t>(op);
        req.count++;

        Transport::Response::BGetOp *res = ds->operate(&req);
        ASSERT_NE(res, nullptr);
        EXPECT_EQ(res->count, req.count);

        switch (req.ops[0]) {
            case HXHIM_GET_EQ:
                EXPECT_EQ(res->num_recs[0], 1);

                // all results are the same value
                for(std::size_t j = 0; j < res->num_recs[0]; j++) {
                    ASSERT_NE(res->subjects[0][j], nullptr);
                    ASSERT_NE(res->subjects[0][j]->data(), nullptr);
                    EXPECT_EQ(memcmp(subs[0],  res->subjects[0][j]->data(),   res->subjects[0][j]->size()),   0);
                    ASSERT_NE(res->predicates[0][j], nullptr);
                    ASSERT_NE(res->predicates[0][j]->data(), nullptr);
                    EXPECT_EQ(memcmp(preds[0], res->predicates[0][j]->data(), res->predicates[0][j]->size()), 0);
                    ASSERT_NE(res->objects[0][j], nullptr);
                    ASSERT_NE(res->objects[0][j]->data(), nullptr);
                    EXPECT_EQ(memcmp(objs[0],  res->objects[0][j]->data(),    res->objects[0][j]->size()),    0);
                }
                break;
            case HXHIM_GET_NEXT:
            case HXHIM_GET_FIRST:
                EXPECT_EQ(res->num_recs[0], count);

                // only 2 values available
                for(std::size_t j = 0; j < res->num_recs[0]; j++) {
                    ASSERT_NE(res->subjects[0][j], nullptr);
                    ASSERT_NE(res->subjects[0][j]->data(), nullptr);
                    EXPECT_EQ(memcmp(subs[j],  res->subjects[0][j]->data(),   res->subjects[0][j]->size()),   0);
                    ASSERT_NE(res->predicates[0][j], nullptr);
                    ASSERT_NE(res->predicates[0][j]->data(), nullptr);
                    EXPECT_EQ(memcmp(preds[j], res->predicates[0][j]->data(), res->predicates[0][j]->size()), 0);
                    ASSERT_NE(res->objects[0][j], nullptr);
                    ASSERT_NE(res->objects[0][j]->data(), nullptr);
                    EXPECT_EQ(memcmp(objs[j],  res->objects[0][j]->data(),    res->objects[0][j]->size()),    0);
                }
                break;
            case HXHIM_GET_PREV:
                EXPECT_EQ(res->num_recs[0], 1);

                for(std::size_t j = 0; j < res->num_recs[0]; j++) {
                    ASSERT_NE(res->subjects[0][j], nullptr);
                    ASSERT_NE(res->subjects[0][j]->data(), nullptr);
                    EXPECT_EQ(memcmp(subs[j],  res->subjects[0][j]->data(),   res->subjects[0][j]->size()),   0);
                    ASSERT_NE(res->predicates[0][j], nullptr);
                    ASSERT_NE(res->predicates[0][j]->data(), nullptr);
                    EXPECT_EQ(memcmp(preds[j], res->predicates[0][j]->data(), res->predicates[0][j]->size()), 0);
                    ASSERT_NE(res->objects[0][j], nullptr);
                    ASSERT_NE(res->objects[0][j]->data(), nullptr);
                    EXPECT_EQ(memcmp(objs[j],  res->objects[0][j]->data(),    res->objects[0][j]->size()),    0);
                }
                break;
            case HXHIM_GET_LAST:
                EXPECT_EQ(res->num_recs[0], count);

                for(std::size_t j = 0; j < res->num_recs[0]; j++) {
                    const std::size_t k = res->num_recs[0] - 1 - j;;

                    ASSERT_NE(res->subjects[0][k], nullptr);
                    ASSERT_NE(res->subjects[0][k]->data(), nullptr);
                    EXPECT_EQ(memcmp(subs[j],  res->subjects[0][k]->data(),   res->subjects[0][k]->size()),   0);
                    ASSERT_NE(res->predicates[0][j], nullptr);
                    ASSERT_NE(res->predicates[0][j]->data(), nullptr);
                    EXPECT_EQ(memcmp(preds[j], res->predicates[0][k]->data(), res->predicates[0][k]->size()), 0);
                    ASSERT_NE(res->objects[0][j], nullptr);
                    ASSERT_NE(res->objects[0][j]->data(), nullptr);
                    EXPECT_EQ(memcmp(objs[j],  res->objects[0][k]->data(),    res->objects[0][k]->size()),    0);
                }
                break;
            case HXHIM_GET_INVALID:
            default:
                break;
        }

        destruct(res);
    }

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
        ReferenceBlob sub((void *) subs[i], strlen(subs[i]));
        ReferenceBlob pred((void *) preds[i], strlen(preds[i]));
        void * key = nullptr;
        std::size_t key_len = 0;
        EXPECT_EQ(sp_to_key(&sub, &pred,
                            &key, &key_len), HXHIM_SUCCESS);

        EXPECT_EQ(db.find(std::string((char *) key, key_len)) == db.end(), true);
        dealloc(key);
    }

    destruct(ds);
}
