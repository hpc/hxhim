#include <cstring>

#include <gtest/gtest.h>

#include "common.hpp"
#include "datastore/InMemory.hpp"
#include "hxhim/triplestore.hpp"
#include "utils/memory.hpp"

class InMemoryTest : public datastore::InMemory {
    public:
        InMemoryTest()
            : datastore::InMemory(-1, 0, nullptr, "InMemory test")
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
        req.subjects[i]     = triples[i].get_sub();
        req.predicates[i]   = triples[i].get_pred();
        req.objects[i]      = triples[i].get_obj();
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

    std::size_t key_buffer_len = all_keys_size();
    char *key_buffer = (char *) alloc(key_buffer_len);
    char *key_buffer_start = key_buffer;

    for(Triple const &triple : triples) {
        std::size_t key_len = 0;
        char *key = sp_to_key(triple.get_sub(), triple.get_pred(), key_buffer, key_buffer_len, key_len);
        EXPECT_NE(key, nullptr);

        EXPECT_EQ(db.find(std::string(key, key_len)) != db.end(), true);
    }

    destruct(key_buffer_start);
    destruct(ds);
}

TEST(InMemory, BGet) {
    InMemoryTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    Transport::Request::BGet req(count + 1);
    for(std::size_t i = 0; i < count; i++) {
        req.subjects[i]     = triples[i].get_sub();
        req.predicates[i]   = triples[i].get_pred();
        req.count++;
    }

    // non-existant subject-predicate pair
    req.subjects[count]     = ReferenceBlob((void *) "sub3",  4, hxhim_data_t::HXHIM_DATA_BYTE);
    req.predicates[count]   = ReferenceBlob((void *) "pred3", 5, hxhim_data_t::HXHIM_DATA_BYTE);
    req.object_types[count] = hxhim_data_t::HXHIM_DATA_BYTE;
    req.count++;

    Transport::Response::BGet *res = ds->operate(&req);
    ASSERT_NE(res, nullptr);
    EXPECT_EQ(res->count, count + 1);
    for(std::size_t i = 0; i < count; i++) {
        EXPECT_EQ(res->statuses[i], DATASTORE_SUCCESS);
        ASSERT_NE(res->objects[i].data(), nullptr);
        EXPECT_EQ(res->objects[i].size(), triples[i].get_obj().size());
        EXPECT_EQ(std::memcmp(triples[i].get_obj().data(), res->objects[i].data(), res->objects[i].size()), 0);
    }

    EXPECT_EQ(res->statuses[count], DATASTORE_ERROR);
    EXPECT_EQ(res->objects[count].data(), nullptr);

    destruct(res);
    destruct(ds);
}

TEST(InMemory, BGetOp) {
    InMemoryTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    std::size_t key_buffer_len = all_keys_size();
    char *key_buffer = (char *) alloc(key_buffer_len);
    char *key_buffer_start = key_buffer;

    for(int op = HXHIM_GETOP_EQ; op < HXHIM_GETOP_INVALID; op++) {
        Transport::Request::BGetOp req(1);

        req.subjects[0]     = triples[0].get_sub();
        req.predicates[0]   = triples[0].get_pred();
        req.object_types[0] = triples[0].get_obj().data_type();
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
                    EXPECT_EQ(memcmp(triples[0].get_sub().data(),
                                     res->subjects[0][j].data(),
                                     res->subjects[0][j].size()),   0);

                    ASSERT_NE(res->predicates[0][j].data(), nullptr);
                    EXPECT_EQ(memcmp(triples[0].get_pred().data(),
                                     res->predicates[0][j].data(),
                                     res->predicates[0][j].size()), 0);

                    ASSERT_NE(res->objects[0][j].data(), nullptr);
                    EXPECT_EQ(memcmp(triples[0].get_obj().data(),
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
                    EXPECT_EQ(memcmp(triples[j].get_sub().data(),
                                     res->subjects[0][j].data(),
                                     res->subjects[0][j].size()),   0);

                    ASSERT_NE(res->predicates[0][j].data(), nullptr);
                    EXPECT_EQ(memcmp(triples[j].get_pred().data(),
                                     res->predicates[0][j].data(),
                                     res->predicates[0][j].size()), 0);

                    ASSERT_NE(res->objects[0][j].data(), nullptr);
                    EXPECT_EQ(memcmp(triples[j].get_obj().data(),
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

    destruct(key_buffer_start);
    destruct(ds);
}

TEST(InMemory, BDelete) {
    InMemoryTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    std::map<std::string, std::string> const &db = ds->data();
    EXPECT_EQ(db.size(), count);

    Transport::Request::BDelete req(count + 1);
    for(std::size_t i = 0; i < count; i++) {
        req.subjects[i]     = triples[i].get_sub();
        req.predicates[i]   = triples[i].get_pred();
        req.count++;
    }

    // non-existant subject-predicate pair
    req.subjects[count]     = ReferenceBlob((void *) "sub3",  4, hxhim_data_t::HXHIM_DATA_BYTE);
    req.predicates[count]   = ReferenceBlob((void *) "pred3", 5, hxhim_data_t::HXHIM_DATA_BYTE);
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

    std::size_t key_buffer_len = all_keys_size();
    char *key_buffer = (char *) alloc(key_buffer_len);
    char *key_buffer_start = key_buffer;

    // get directly from internal data
    for(Triple const &triple : triples) {
        std::size_t key_len = 0;
        char *key = sp_to_key(triple.get_sub(), triple.get_pred(), key_buffer, key_buffer_len, key_len);
        EXPECT_NE(key, nullptr);

        EXPECT_EQ(db.find(std::string(key, key_len)) == db.end(), true);
    }

    destruct(key_buffer_start);
    destruct(ds);
}
