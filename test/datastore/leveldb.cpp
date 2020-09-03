#include <cstring>
#include <dirent.h>
#include <sstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <leveldb/db.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "datastore/leveldb.hpp"
#include "utils/memory.hpp"
#include "utils/triplestore.hpp"

class LevelDBTest : public hxhim::datastore::leveldb {
    public:
        LevelDBTest(const int rank, const std::string &name)
            : hxhim::datastore::leveldb(rank, 0, name, true)
        {}

        ~LevelDBTest()  {
            Close();
            cleanup();
        }

        ::leveldb::DB *data() const {
            return db;
        }

    private:
        void cleanup() {
            remove(dbname.c_str());
        }
};

void rm_r(const std::string &path, const char sep = '/') {
    // checking for . and .. to be deleted here
    // would allow for entries that end with them
    // to be skipped erroneously without using
    // basename(3).
    //
    // This also allows for . and .. to be deleted.
    struct stat st;
    if (stat(path.c_str(), &st) == 0) {
        if (S_ISDIR(st.st_mode)) {

            DIR *dir = opendir(path.c_str());

            struct dirent *entry = nullptr;
            while ((entry = readdir(dir))) {
                const std::size_t len = strlen(entry->d_name);
                if (((len == 1) && (memcmp(entry->d_name, ".",  1) == 0))  ||
                    ((len == 2) && (memcmp(entry->d_name, "..", 2) == 0)))  {
                    continue;
                }

                const std::string sub = path + sep + entry->d_name;
                rm_r(sub, sep);
            }

            closedir(dir);
        }
    }

    remove(path.c_str());
}

// create a test LevelDB datastore and insert some triples
static LevelDBTest *setup() {
    int rank = -1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    std::stringstream s;
    s << "LEVELDB-TEST-" << rank;

    rm_r(s.str());

    LevelDBTest *ds = construct<LevelDBTest>(rank, s.str());

    Transport::Request::BPut req(count);

    for(std::size_t i = 0; i < count; i++) {
        req.subjects[i]     = triples[i].get_sub();
        req.predicates[i]   = triples[i].get_pred();
        req.object_types[i] = triples[i].get_type();
        req.objects[i]      = triples[i].get_obj();
        req.count++;
    }

    Transport::Response::BPut * res = ds->operate(&req);
    destruct(res);

    return ds;
}

TEST(LevelDB, BPut) {
    LevelDBTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    ::leveldb::DB *db = ds->data();

    std::size_t key_buffer_len = all_keys_size();
    char *key_buffer = (char *) alloc(key_buffer_len);
    char *key_buffer_start = key_buffer;

    // read directly from leveldb since setup() already did PUTs
    for(std::size_t i = 0; i < count; i++) {
        std::size_t key_len = 0;
        char *key = sp_to_key(triples[i].get_sub(), triples[i].get_pred(), key_buffer, key_buffer_len, key_len);
        EXPECT_NE(key, nullptr);

        std::string k(key, key_len);
        std::string v;
        leveldb::Status status = db->Get(leveldb::ReadOptions(), k, &v);
        EXPECT_EQ(status.ok(), true);
        EXPECT_EQ(memcmp(triples[i].get_obj().data(), v.c_str(), v.size()), 0);
    }

    // make sure datastore only has count items
    std::size_t items = 0;
    leveldb::Iterator *it = db->NewIterator(leveldb::ReadOptions());
    for(it->SeekToFirst(); it->Valid(); it->Next()) {
        items++;
    }
    EXPECT_EQ(it->status().ok(), true);
    EXPECT_EQ(items, total);
    delete it;

    destruct(key_buffer_start);
    destruct(ds);
}

TEST(LevelDB, BGet) {
    LevelDBTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    // get triple back using GET
    Transport::Request::BGet req(count + 1);
    for(std::size_t i = 0; i < count; i++) {
        req.subjects[i]     = triples[i].get_sub();
        req.predicates[i]   = triples[i].get_pred();
        req.object_types[i] = triples[i].get_type();
        req.count++;
    }

    // non-existant subject-predicate pair
    req.subjects[count]     = ReferenceBlob((void *) "sub3",  4);
    req.predicates[count]   = ReferenceBlob((void *) "pred3", 5);
    req.object_types[count] = hxhim_object_type_t::HXHIM_OBJECT_TYPE_BYTE;
    req.count++;

    Transport::Response::BGet *res = ds->operate(&req);
    ASSERT_NE(res, nullptr);
    EXPECT_EQ(res->count, count + 1);
    for(std::size_t i = 0; i < count; i++) {
        EXPECT_EQ(res->statuses[i], HXHIM_SUCCESS);
        ASSERT_NE(res->objects[i].data(), nullptr);
        EXPECT_EQ(res->objects[i].size(), triples[i].get_obj().size());
        EXPECT_EQ(std::memcmp(triples[i].get_obj().data(), res->objects[i].data(), res->objects[i].size()), 0);
    }

    EXPECT_EQ(res->statuses[count], HXHIM_ERROR);
    EXPECT_EQ(res->objects[count].data(), nullptr);

    destruct(res);
    destruct(ds);
}

TEST(LevelDB, BGetOp) {
    LevelDBTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    for(int op = HXHIM_GET_EQ; op < HXHIM_GET_INVALID; op++) {
        Transport::Request::BGetOp req(1);
        req.subjects[0]     = triples[0].get_sub();
        req.predicates[0]   = triples[0].get_pred();
        req.object_types[0] = triples[0].get_type();
        req.num_recs[0]     = 1;
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
            case HXHIM_GET_NEXT:
            case HXHIM_GET_FIRST:
            case HXHIM_GET_LAST:
                EXPECT_EQ(res->num_recs[0], 1);
                // not sure how to test these
                break;
            case HXHIM_GET_PREV:
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
            case HXHIM_GET_INVALID:
            default:
                break;
        }

        destruct(res);
    }

    destruct(ds);
}

TEST(LevelDB, BDelete) {
    LevelDBTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    ::leveldb::DB *db = ds->data();

    // delete the triples
    {
        Transport::Request::BDelete req(count + 1);
        for(std::size_t i = 0; i < count; i++) {
            req.subjects[i]     = triples[i].get_sub();
            req.predicates[i]   = triples[i].get_pred();
            req.count++;
        }

        // non-existant subject-predicate pair
        req.subjects[count]     = ReferenceBlob((void *) "sub3",  4);
        req.predicates[count]   = ReferenceBlob((void *) "pred3", 5);
        req.count++;

        Transport::Response::BDelete *res = ds->operate(&req);
        ASSERT_NE(res, nullptr);
        EXPECT_EQ(res->count, count + 1);
        for(std::size_t i = 0; i < count; i++) {
            EXPECT_EQ(res->statuses[i], HXHIM_SUCCESS);
        }

        // as long as one delete succeeded, will return HXHIM_SUCCESS
        EXPECT_EQ(res->statuses[count], HXHIM_SUCCESS);

        destruct(res);
    }

    // check if the triples still exist using GET
    {
        Transport::Request::BGet req(count + 1);
        for(std::size_t i = 0; i < count; i++) {
            req.subjects[i]     = triples[i].get_sub();
            req.predicates[i]   = triples[i].get_pred();
            req.count++;
        }

        // non-existant subject-predicate pair
        req.subjects[count]   = ReferenceBlob((void *) "sub3",  4);
        req.predicates[count] = ReferenceBlob((void *) "pred3", 5);
        req.count++;

        Transport::Response::BGet *res = ds->operate(&req);
        ASSERT_NE(res, nullptr);
        EXPECT_EQ(res->count, count + 1);
        for(std::size_t i = 0; i < res->count; i++) {
            EXPECT_EQ(res->statuses[i], HXHIM_ERROR);
        }

        destruct(res);
    }

    // check the datastore directly
    {
        std::size_t key_buffer_len = all_keys_size();
        char *key_buffer = (char *) alloc(key_buffer_len);
        char *key_buffer_start = key_buffer;

        for(std::size_t i = 0; i < count; i++) {
            std::size_t key_len = 0;
            char *key = sp_to_key(triples[i].get_sub(), triples[i].get_pred(), key_buffer, key_buffer_len, key_len);
            EXPECT_NE(key, nullptr);

            std::string k(key, key_len);
            std::string v;
            leveldb::Status status = db->Get(leveldb::ReadOptions(), k, &v);
            EXPECT_EQ(status.ok(), false);
            EXPECT_EQ(memcmp(triples[i].get_obj().data(), v.c_str(), v.size()), 0);
        }

        destruct(key_buffer_start);
    }

    // make sure datastore doesn't have the original SPO triples
    {
        std::size_t items = 0;
        leveldb::Iterator *it = db->NewIterator(leveldb::ReadOptions());
        for(it->SeekToFirst(); it->Valid(); it->Next()) {
            items++;
        }
        EXPECT_EQ(it->status().ok(), true);
        EXPECT_EQ(items, total - count);
        delete it;
    }

    destruct(ds);
}
