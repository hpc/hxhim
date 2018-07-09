#include <utility>
#include <stdexcept>
#include <iostream>

#include "hxhim/backend/mdhim.hpp"
#include "hxhim/triplestore.hpp"

namespace hxhim {
namespace backend {

const std::map<enum hxhim_get_op, enum TransportGetMessageOp> mdhim::GET_OP_MAPPING = {
    std::make_pair(hxhim_get_op::HXHIM_GET_EQ,         TransportGetMessageOp::GET_EQ),
    std::make_pair(hxhim_get_op::HXHIM_GET_NEXT,       TransportGetMessageOp::GET_NEXT),
    std::make_pair(hxhim_get_op::HXHIM_GET_PREV,       TransportGetMessageOp::GET_PREV),
    std::make_pair(hxhim_get_op::HXHIM_GET_FIRST,      TransportGetMessageOp::GET_FIRST),
    std::make_pair(hxhim_get_op::HXHIM_GET_LAST,       TransportGetMessageOp::GET_LAST),
    std::make_pair(hxhim_get_op::HXHIM_GET_PRIMARY_EQ, TransportGetMessageOp::GET_PRIMARY_EQ),
    std::make_pair(hxhim_get_op::HXHIM_GET_OP_MAX,     TransportGetMessageOp::GET_OP_MAX),
};

mdhim::mdhim(MPI_Comm comm, const std::string &config)
    : md(nullptr), mdhim_opts(nullptr)
{
    if (Open(comm, config) != HXHIM_SUCCESS) {
        throw std::runtime_error("Could not configure MDHIM backend");
    }
}

mdhim::~mdhim() {
    Close();
}

int mdhim::Open(MPI_Comm comm, const std::string &config) {
    // fill in the MDHIM options
    mdhim_opts = new mdhim_options_t();
    if (!mdhim_opts                                           ||
        mdhim_default_config_reader(mdhim_opts, comm, config)) {
        mdhim_options_destroy(mdhim_opts);
        delete mdhim_opts;
        return HXHIM_ERROR;
    }

    // fill in the mdhim_t structure
    md = new mdhim_t();
    if (!md                                            ||
        (::mdhim::Init(md, mdhim_opts) != MDHIM_SUCCESS)) {
        ::mdhim::Close(md);
        delete md;
        mdhim_options_destroy(mdhim_opts);
        delete mdhim_opts;
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

void mdhim::Close() {
    ::mdhim::Close(md);
    delete md;
    md = nullptr;
    mdhim_options_destroy(mdhim_opts);
    delete mdhim_opts;
    mdhim_opts = nullptr;
}

/**
 * Commit
 * Syncs the MDHIM databases to disc
 *
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int mdhim::Commit() {
    return (::mdhim::Commit(md, nullptr) == MDHIM_SUCCESS)?HXHIM_SUCCESS:HXHIM_ERROR;
}

/**
 * StatFlush
 * Forces MDHIM to synchronize its internal statistics
 *
 * @return HXHIM_ERROR
 */
int mdhim::StatFlush() {
    return (::mdhim::StatFlush(md, nullptr) == MDHIM_SUCCESS)?HXHIM_SUCCESS:HXHIM_ERROR;
}

/**
 * GetStats
 * Collective operation
 * Collects statistics from all HXHIM ranks
 *
 * @param rank           the rank to send to
 * @param get_put_times  whether or not to get put_times
 * @param put_times      the array of put times from each rank
 * @param get_num_puts   whether or not to get num_puts
 * @param num_puts       the array of number of puts from each rank
 * @param get_get_times  whether or not to get get_times
 * @param get_times      the array of get times from each rank
 * @param get_num_gets   whether or not to get num_gets
 * @param num_gets       the array of number of gets from each rank
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int mdhim::GetStats(const int rank,
                    const bool get_put_times, long double *put_times,
                    const bool get_num_puts, std::size_t *num_puts,
                    const bool get_get_times, long double *get_times,
                    const bool get_num_gets, std::size_t *num_gets) {
    return (::mdhim::GetStats(md, rank,
                              get_put_times, put_times,
                              get_num_puts, num_puts,
                              get_get_times, get_times,
                              get_num_gets, num_gets))?HXHIM_SUCCESS:HXHIM_ERROR;
}

/**
 * BPut
 * Performs a bulk PUT in leveldb
 *
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param objects       the objects to put
 * @param object_lens   the lengths of the objects
 * @return pointer to a list of results
 */
Results *mdhim::BPut(void **subjects, std::size_t *subject_lens,
                     void **predicates, std::size_t *predicate_lens,
                     void **objects, std::size_t *object_lens,
                      std::size_t count) {
    Results *res = new Results();

    // create keys from subjects and predicates
    void **keys = new void *[count]();
    std::size_t *key_lens = new std::size_t[count]();
    for(std::size_t i = 0; i < count; i++) {
        sp_to_key(subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &keys[i], &key_lens[i]);
    }

    // send key-value pairs into MDHIM
    TransportBRecvMessage *brm = ::mdhim::BPut(md, nullptr, keys, key_lens, objects, object_lens, count);

    // flatten results
    for(TransportBRecvMessage *curr = brm; curr; curr = curr->next) {
        for(std::size_t i = 0; i < curr->num_keys; i++) {
            int database = -1;
            ::mdhim::ComposeDB(md, &database, curr->src, curr->rs_idx[i]);
            res->AddPut((curr->error == MDHIM_SUCCESS)?HXHIM_SUCCESS:HXHIM_ERROR, database);
        }
    }

    // cleanup
    delete brm;
    for(std::size_t i = 0; i < count; i++) {
        ::operator delete(keys[i]);
    }
    delete [] keys;
    delete [] key_lens;

    return res;
}

/**
 * BGet
 * Performs a bulk GET in leveldb
 *
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @return pointer to a list of results
 */
Results *mdhim::BGet(void **subjects, std::size_t *subject_lens,
                     void **predicates, std::size_t *predicate_lens,
                     std::size_t count) {
    Results *res = new Results();

    // create keys from subjects and predicates
    void **keys = new void *[count]();
    std::size_t *key_lens = new std::size_t[count]();
    for(std::size_t i = 0; i < count; i++) {
        sp_to_key(subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &keys[i], &key_lens[i]);
    }

    // request values from MDHIM
    TransportBGetRecvMessage *bgrm = ::mdhim::BGet(md, nullptr, keys, key_lens, count, GET_OP_MAPPING.at(hxhim_get_op::HXHIM_GET_EQ));

    // flatten results
    for(TransportBGetRecvMessage *curr = bgrm; curr; curr = curr->next) {
        for(std::size_t i = 0; i < curr->num_keys; i++) {
            // convert the key into a subject predicate pair
            void *temp_sub = nullptr;
            std::size_t sub_len = 0;
            void *temp_pred = nullptr;
            std::size_t pred_len = 0;

            key_to_sp(curr->keys[i], curr->key_lens[i], &temp_sub, &sub_len, &temp_pred, &pred_len);

            void *sub = ::operator new(sub_len);
            memcpy(sub, temp_sub, sub_len);

            void *pred = ::operator new(pred_len);
            memcpy(pred, temp_pred, pred_len);

            void *obj = ::operator new(curr->value_lens[i]);
            memcpy(obj, curr->values[i], curr->value_lens[i]);

            int database = -1;
            ::mdhim::ComposeDB(md, &database, curr->src, curr->rs_idx[i]);

            res->AddGet((curr->error == MDHIM_SUCCESS)?HXHIM_SUCCESS:HXHIM_ERROR, database, sub, sub_len, pred, pred_len, obj, curr->value_lens[i]);
        }
    }

    // cleanup
    delete bgrm;
    for(std::size_t i = 0; i < count; i++) {
        ::operator delete(keys[i]);
    }
    delete [] keys;
    delete [] key_lens;

    return res;
}

/**
 * BGetOp
 * Performs a GetOp in leveldb
 *
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param objects       the objects to put
 * @param object_lens   the lengths of the objects
 * @return pointer to a list of results
 */
Results *mdhim::BGetOp(void *subject, std::size_t subject_len,
                       void *predicate, std::size_t predicate_len,
                       std::size_t count, enum hxhim_get_op op) {
    Results *res = new Results();

    // create key from subjects and predicates
    void *key = nullptr;
    std::size_t key_len = 0;
    sp_to_key(subject, subject_len, predicate, predicate_len, &key, &key_len);

    // request values from MDHIM
    TransportBGetRecvMessage *bgrm = ::mdhim::BGetOp(md, nullptr, key, key_len, count, GET_OP_MAPPING.at(op));

    // flatten results
    for(TransportBGetRecvMessage *curr = bgrm; curr; curr = curr->next) {
        for(std::size_t i = 0; i < curr->num_keys; i++) {
            // convert the key into a subject predicate pair
            void *temp_sub = nullptr;
            std::size_t sub_len = 0;
            void *temp_pred = nullptr;
            std::size_t pred_len = 0;

            key_to_sp(curr->keys[i], curr->key_lens[i], &temp_sub, &sub_len, &temp_pred, &pred_len);

            void *sub = ::operator new(sub_len);
            memcpy(sub, temp_sub, sub_len);

            void *pred = ::operator new(pred_len);
            memcpy(pred, temp_pred, pred_len);

            void *obj = ::operator new(curr->value_lens[i]);
            memcpy(obj, curr->values[i], curr->value_lens[i]);

            int database = -1;
            ::mdhim::ComposeDB(md, &database, curr->src, curr->rs_idx[i]);

            res->AddGet((curr->error == MDHIM_SUCCESS)?HXHIM_SUCCESS:HXHIM_ERROR, database, sub, sub_len, pred, pred_len, obj, curr->value_lens[i]);
        }
    }

    delete bgrm;
    ::operator delete(key);

    return res;
}

/**
 * BDelete
 * Performs a bulk DELETE in leveldb
 *
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @return pointer to a list of results
 */
Results *mdhim::BDelete(void **subjects, std::size_t *subject_lens,
                        void **predicates, std::size_t *predicate_lens,
                        std::size_t count) {
    Results *res = new Results();

    // create keys from subjects and predicates
    void **keys = new void *[count]();
    std::size_t *key_lens = new std::size_t[count]();
    for(std::size_t i = 0; i < count; i++) {
        sp_to_key(subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &keys[i], &key_lens[i]);
    }

    // delete values from MDHIM
    TransportBRecvMessage *brm = ::mdhim::BDelete(md, nullptr, keys, key_lens, count);

    // flatten results
    for(TransportBRecvMessage *curr = brm; curr; curr = curr->next) {
        for(std::size_t i = 0; i < curr->num_keys; i++) {
            int database = -1;
            ::mdhim::ComposeDB(md, &database, curr->src, curr->rs_idx[i]);
            res->AddDelete((curr->error == MDHIM_SUCCESS)?HXHIM_SUCCESS:HXHIM_ERROR, database);
        }
    }

    // cleanup
    delete brm;
    for(std::size_t i = 0; i < count; i++) {
        ::operator delete(keys[i]);
    }
    delete [] keys;
    delete [] key_lens;

    return res;
}

}
}