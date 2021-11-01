#include "hxhim/hxhim.hpp"
#include "hxhim/private/hxhim.hpp"
#include "utils/Blob.hpp"

/**
 * Put
 * Add a PUT into the work queue
 *
 * @param hx             the HXHIM session
 * @param subject        the subject to put
 * @param subject_len    the length of the subject to put
 * @param predicate      the prediate to put
 * @param predicate_len  the length of the prediate to put
 * @param object_type    the type of the object
 * @param object         the object to put
 * @param object_len     the length of the object
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Put(hxhim_t *hx,
               void *subject, std::size_t subject_len, enum hxhim_data_t subject_type,
               void *predicate, std::size_t predicate_len, enum hxhim_data_t predicate_type,
               void *object, std::size_t object_len, enum hxhim_data_t object_type,
               const hxhim_put_permutation_t permutations) {
    mlog(HXHIM_CLIENT_DBG, "%s %s:%d", __FILE__, __func__, __LINE__);
    if (!valid(hx) || !hx->p->running ||
        !subject   || !subject_len    ||
        !predicate || !predicate_len  ||
        !object    || !object_len)     {
        return HXHIM_ERROR;
    }

    int rc = HXHIM_ERROR;

    ::Stats::Chronostamp put;
    put.start = ::Stats::now();

    if (hx->p->async_puts.enabled) {
        hx->p->queues.puts.mutex.lock();
    }

    rc = hxhim::PutImpl(hx,
                        hx->p->queues.puts.queue,
                        ReferenceBlob(subject, subject_len, subject_type),
                        ReferenceBlob(predicate, predicate_len, predicate_type),
                        ReferenceBlob(object, object_len, object_type),
                        permutations);

    if (hx->p->async_puts.enabled) {
        hx->p->queues.puts.start_processing.notify_all();
        hx->p->queues.puts.mutex.unlock();
    }

    put.end = ::Stats::now();
    hx->p->stats.single_op[hxhim_op_t::HXHIM_PUT].emplace_back(put);
    return rc;
}

/**
 * hxhimPut
 * Add a PUT into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to put
 * @param subject_len  the length of the subject to put
 * @param prediate     the prediate to put
 * @param prediate_len the length of the prediate to put
 * @param object_type  the type of the object
 * @param object       the object to put
 * @param object_len   the length of the object
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimPut(hxhim_t *hx,
             void *subject, size_t subject_len, enum hxhim_data_t subject_type,
             void *predicate, size_t predicate_len, enum hxhim_data_t predicate_type,
             void *object, size_t object_len, enum hxhim_data_t object_type,
             const hxhim_put_permutation_t permutations) {
    mlog(HXHIM_CLIENT_DBG, "%s %s:%d", __FILE__, __func__, __LINE__);
    return hxhim::Put(hx,
                      subject, subject_len, subject_type,
                      predicate, predicate_len, predicate_type,
                      object, object_len, object_type,
                      permutations);
}
