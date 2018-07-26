#include <cstring>
#include <stdexcept>
#include <utility>

#include "hxhim/backend/mdhim.hpp"
#include "hxhim/private.hpp"
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

mdhim::mdhim(hxhim_t *hx, const std::string &config)
    : base(hx),
      config_filename(config),
      md(nullptr), mdhim_opts(nullptr)
{
    if (Open(hx->mpi.comm, config_filename) != HXHIM_SUCCESS) {
        throw std::runtime_error("Could not configure MDHIM backend");
    }
}

mdhim::~mdhim() {
    Close();
}

int mdhim::Open(MPI_Comm comm, const std::string &config) {
    // fill in the MDHIM options
    if (!(mdhim_opts = new mdhim_options_t())                                  ||
        mdhim_default_config_reader(mdhim_opts, comm, config) != MDHIM_SUCCESS) {
        Close();
        return HXHIM_ERROR;
    }

    // fill in the mdhim_t structure
    if (!(md = new mdhim_t())                            ||
        (::mdhim::Init(md, mdhim_opts) != MDHIM_SUCCESS)) {
        Close();
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
            res->Add(new Results::Put((curr->error == MDHIM_SUCCESS)?HXHIM_SUCCESS:HXHIM_ERROR, database));
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
                     hxhim_spo_type_t *object_types,
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
            // get the database number
            int database = -1;
            ::mdhim::ComposeDB(md, &database, curr->src, curr->rs_idx[i]);

            res->Add(new GetResult(curr->error, database,
                                   object_types[i],
                                   curr->keys[i], curr->key_lens[i],
                                   curr->values[i], curr->value_lens[i]));
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
                       hxhim_spo_type_t object_type,
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
            // get the database number
            int database = -1;
            ::mdhim::ComposeDB(md, &database, curr->src, curr->rs_idx[i]);

            res->Add(new GetResult(curr->error, database,
                                   object_type,
                                   curr->keys[i], curr->key_lens[i],
                                   curr->values[i], curr->value_lens[i]));
            curr->keys[i] = nullptr;
            curr->values[i] = nullptr;
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
            res->Add(new Results::Delete((curr->error == MDHIM_SUCCESS)?HXHIM_SUCCESS:HXHIM_ERROR, database));
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

std::ostream &mdhim::print_config(std::ostream &stream) const {
    return stream
        << "mdhim" << std::endl
        << "    config: " << config_filename << std::endl;
}

mdhim::GetResult::GetResult(const int err, const int db,
                            hxhim_spo_type_t object_type,
                            void *key, std::size_t key_len,
                            void *value, std::size_t value_len)
    : Get((err == MDHIM_SUCCESS)?HXHIM_SUCCESS:HXHIM_ERROR, db, object_type),
      k(nullptr), k_len(key_len),
      v(nullptr), v_len(value_len)
{
    k = ::operator new(k_len);
    memcpy(k, key, k_len);

    v = ::operator new(v_len);
    memcpy(v, value, v_len);
}

mdhim::GetResult::~GetResult()
{
    ::operator delete(k);
    ::operator delete(v);
}

int mdhim::GetResult::FillSubject() {
    if (!sub) {
        key_to_sp(k, k_len, &sub, &sub_len, nullptr, nullptr);
    }

    return HXHIM_SUCCESS;
}

int mdhim::GetResult::FillPredicate() {
    if (!pred) {
        key_to_sp(k, k_len, nullptr, nullptr, &pred, &pred_len);
    }

    return HXHIM_SUCCESS;
}

int mdhim::GetResult::FillObject() {
    if (!obj) {
        return decode(obj_type, v, v_len, &obj, &obj_len);
    }

    return HXHIM_SUCCESS;
}

}
}
