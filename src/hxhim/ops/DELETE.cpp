#include "hxhim/hxhim.hpp"
#include "hxhim/private/hxhim.hpp"
#include "utils/Blob.hpp"

/**
 * Delete
 * Add a DELETE into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to delete
 * @param subject_len  the length of the subject to delete
 * @param prediate     the prediate to delete
 * @param prediate_len the length of the prediate to delete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Delete(hxhim_t *hx,
                  void *subject, std::size_t subject_len,
                  void *predicate, std::size_t predicate_len) {
    if (!valid(hx) || !hx->p->running) {
        return HXHIM_ERROR;
    }

    ::Stats::Chronostamp del;
    del.start = ::Stats::now();
    const int rc = hxhim::DeleteImpl(hx,
                                     hx->p->queues.deletes,
                                     ReferenceBlob(subject, subject_len),
                                     ReferenceBlob(predicate, predicate_len));
    del.end = ::Stats::now();
    hx->p->stats.single_op[hxhim_op_t::HXHIM_DELETE].emplace_back(del);
    return rc;
}

/**
 * hxhimDelete
 * Add a DELETE into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to delete
 * @param subject_len  the length of the subject to delete
 * @param prediate     the prediate to delete
 * @param prediate_len the length of the prediate to delete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimDelete(hxhim_t *hx,
                void *subject, size_t subject_len,
                void *predicate, size_t predicate_len) {
    return hxhim::Delete(hx,
                         subject, subject_len,
                         predicate, predicate_len);
}
