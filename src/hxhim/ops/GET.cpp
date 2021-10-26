#include "hxhim/hxhim.hpp"
#include "hxhim/private/hxhim.hpp"
#include "utils/Blob.hpp"

/**
 * Get
 * Add a GET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subject        the subject to put
 * @param subject_len    the length of the subject to put
 * @param predicate      the prediate to put
 * @param predicate_len  the length of the prediate to put
 * @param object_type    the type of the object
 * @param object         the prediate to put
 * @param object_len     the length of the prediate to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Get(hxhim_t *hx,
               void *subject, std::size_t subject_len, enum hxhim_data_t subject_type,
               void *predicate, std::size_t predicate_len, enum hxhim_data_t predicate_type,
               enum hxhim_data_t object_type) {
    if (!valid(hx) || !hx->p->running ||
        !subject   || !subject_len    ||
        !predicate || !predicate_len) {
        return HXHIM_ERROR;
    }

    ::Stats::Chronostamp get;
    get.start = ::Stats::now();
    const int rc = hxhim::GetImpl(hx,
                                  hx->p->queues.gets,
                                  ReferenceBlob(subject, subject_len, subject_type),
                                  ReferenceBlob(predicate, predicate_len, predicate_type),
                                  object_type);
    get.end = ::Stats::now();
    hx->p->stats.single_op[hxhim_op_t::HXHIM_GET].emplace_back(get);
    return rc;
}

/**
 * Get
 * Add a GET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subject        the subject to put
 * @param subject_len    the length of the subject to put
 * @param predicate      the prediate to put
 * @param predicate_len  the length of the prediate to put
 * @param object_type    the type of the object
 * @param object         the prediate to put
 * @param object_len     the length of the prediate to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimGet(hxhim_t *hx,
             void *subject, size_t subject_len, enum hxhim_data_t subject_type,
             void *predicate, size_t predicate_len, enum hxhim_data_t predicate_type,
             enum hxhim_data_t object_type) {
    return hxhim::Get(hx,
                      subject, subject_len, subject_type,
                      predicate, predicate_len, predicate_type,
                      object_type);
}
