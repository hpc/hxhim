#include <cstring>

#include <gtest/gtest.h>

#include "datastore/InMemory.hpp"
#include "hxhim/triplestore.hpp"
#include "triples.hpp"
#include "utils/memory.hpp"

class InMemoryTest : public datastore::InMemory {
  public:
    InMemoryTest()
        : datastore::InMemory(-1, 0, nullptr, nullptr, "InMemory test")
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
        req.subjects[i]   = BLOB(subjects[i]);
        req.predicates[i] = BLOB(predicates[i]);
        req.objects[i]    = BLOB(objects[i]);
        req.count++;
    }

    Transport::Response::BPut *res = ds->operate(&req);
    destruct(res);

    return ds;
}

TEST(InMemory, BPut) {
    InMemoryTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    std::map<std::string, std::string> const &db = ds->data();
    EXPECT_EQ(db.size(), count);

    for(std::size_t i = 0; i < count; i++) {
        std::string key;
        EXPECT_EQ(sp_to_key(BLOB(subjects[i]), BLOB(predicates[i]), key), HXHIM_SUCCESS);
        EXPECT_EQ(key.size(),
                  BLOB(subjects[i]).pack_size(false) +
                  BLOB(predicates[i]).pack_size(false));
        EXPECT_EQ(db.find(key) != db.end(), true);
    }

    destruct(ds);
}

TEST(InMemory, BGet) {
    InMemoryTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    Transport::Request::BGet req(count + 1);
    for(std::size_t i = 0; i < count; i++) {
        req.subjects[i]     = BLOB(subjects[i]);
        req.predicates[i]   = BLOB(predicates[i]);
        req.object_types[i] = hxhim_data_t::HXHIM_DATA_BYTE;
        req.count++;
    }

    // non-existant subject-predicate pair
    req.subjects[count]     = BLOB(std::string("sub3"));
    req.predicates[count]   = BLOB(std::string("pred3"));
    req.object_types[count] = hxhim_data_t::HXHIM_DATA_BYTE;
    req.count++;

    Transport::Response::BGet *res = ds->operate(&req);
    ASSERT_NE(res, nullptr);
    EXPECT_EQ(res->count, count + 1);
    for(std::size_t i = 0; i < count; i++) {
        EXPECT_EQ(res->statuses[i], DATASTORE_SUCCESS);
        ASSERT_NE(res->objects[i].data(), nullptr);
        EXPECT_EQ(res->objects[i].size(), BLOB(objects[i]).size());
        EXPECT_EQ(std::memcmp(BLOB(objects[i]).data(),
                              res->objects[i].data(),
                              res->objects[i].size()),
                  0);
    }

    EXPECT_EQ(res->statuses[count], DATASTORE_ERROR);
    EXPECT_EQ(res->objects[count].data(), nullptr);

    destruct(res);
    destruct(ds);
}

TEST(InMemory, BGetOp) {
    InMemoryTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    for(int op = HXHIM_GETOP_EQ; op < HXHIM_GETOP_INVALID; op++) {
        Transport::Request::BGetOp req(1);

        req.subjects[0]     = BLOB(subjects[0]);
        req.predicates[0]   = BLOB(predicates[0]);
        req.object_types[0] = hxhim_data_t::HXHIM_DATA_BYTE;
        req.num_recs[0]     = 1;
        req.ops[0]          = static_cast<hxhim_getop_t>(op);
        req.count++;

        Transport::Response::BGetOp *res = ds->operate(&req);
        ASSERT_NE(res, nullptr);
        EXPECT_EQ(res->count, req.count);

        switch (req.ops[0]) {
            case HXHIM_GETOP_EQ:
                EXPECT_EQ(res->num_recs[0], 1);

                // all results are the same value
                for(std::size_t j = 0; j < res->num_recs[0]; j++) {
                    ASSERT_NE(res->subjects[0][j].data(), nullptr);
                    EXPECT_EQ(memcmp(subjects[0].data(),
                                     res->subjects[0][j].data(),
                                     res->subjects[0][j].size()),   0);

                    ASSERT_NE(res->predicates[0][j].data(), nullptr);
                    EXPECT_EQ(memcmp(predicates[0].data(),
                                     res->predicates[0][j].data(),
                                     res->predicates[0][j].size()), 0);

                    ASSERT_NE(res->objects[0][j].data(), nullptr);
                    EXPECT_EQ(memcmp(objects[0].data(),
                                     res->objects[0][j].data(),
                                     res->objects[0][j].size()),    0);
                }
                break;
            case HXHIM_GETOP_NEXT:
            case HXHIM_GETOP_FIRST:
            case HXHIM_GETOP_LAST:
                EXPECT_EQ(res->num_recs[0], 1);
                // not sure how to test these
                break;
            case HXHIM_GETOP_PREV:
                EXPECT_EQ(res->num_recs[0], 1);

                for(std::size_t j = 0; j < res->num_recs[0]; j++) {
                    ASSERT_NE(res->subjects[0][j].data(), nullptr);
                    EXPECT_EQ(memcmp(subjects[j].data(),
                                     res->subjects[0][j].data(),
                                     res->subjects[0][j].size()),   0);

                    ASSERT_NE(res->predicates[0][j].data(), nullptr);
                    EXPECT_EQ(memcmp(predicates[j].data(),
                                     res->predicates[0][j].data(),
                                     res->predicates[0][j].size()), 0);

                    ASSERT_NE(res->objects[0][j].data(), nullptr);
                    EXPECT_EQ(memcmp(objects[j].data(),
                                     res->objects[0][j].data(),
                                     res->objects[0][j].size()),    0);
                }
                break;
            case HXHIM_GETOP_INVALID:
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
    EXPECT_EQ(db.size(), count);

    Transport::Request::BDelete req(count + 1);
    for(std::size_t i = 0; i < count; i++) {
        req.subjects[i]   = BLOB(subjects[i]);
        req.predicates[i] = BLOB(predicates[i]);
        req.count++;
    }

    // non-existant subject-predicate pair
    req.subjects[count]   = BLOB(std::string("sub3"));
    req.predicates[count] = BLOB(std::string("pred3"));
    req.count++;

    Transport::Response::BDelete *res = ds->operate(&req);
    ASSERT_NE(res, nullptr);
    EXPECT_EQ(res->count, count + 1);
    for(std::size_t i = 0; i < count; i++) {
        EXPECT_EQ(res->statuses[i], DATASTORE_SUCCESS);
    }

    EXPECT_EQ(res->statuses[count], DATASTORE_ERROR);

    destruct(res);

    EXPECT_EQ(db.size(), 0);

    // get directly from internal data
    for(std::size_t i = 0; i < count; i++) {
        const Blob sub = BLOB(subjects[i]);
        const Blob pred = BLOB(predicates[i]);
        std::string key;
        EXPECT_EQ(sp_to_key(sub, pred, key), HXHIM_SUCCESS);
        EXPECT_EQ(key.size(), sub.pack_size(false) + pred.pack_size(false));
        EXPECT_EQ(db.find(key) == db.end(), true);
    }

    destruct(ds);
}
