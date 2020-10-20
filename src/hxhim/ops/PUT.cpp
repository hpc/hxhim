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
               void *subject, std::size_t subject_len,
               void *predicate, std::size_t predicate_len,
               enum hxhim_object_type_t object_type, void *object, std::size_t object_len) {
    mlog(HXHIM_CLIENT_DBG, "%s %s:%d", __FILE__, __func__, __LINE__);
    if (!valid(hx) || !hx->p->running) {
        return HXHIM_ERROR;
    }

    ::Stats::Chronostamp put;
    put.start = ::Stats::now();

    const int rc = hxhim::PutImpl(hx,
                                  hx->p->queues.puts.queue,
                                  ReferenceBlob(subject, subject_len),
                                  ReferenceBlob(predicate, predicate_len),
                                  object_type,
                                  ReferenceBlob(object, object_len));

    // #if ASYNC_PUTS
    // hx->p->queues.puts.start_processing.notify_all();
    // #else
    hxhim::serial_puts(hx);
    // #endif

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
             void *subject, size_t subject_len,
             void *predicate, size_t predicate_len,
             enum hxhim_object_type_t object_type, void *object, size_t object_len) {
    mlog(HXHIM_CLIENT_DBG, "%s %s:%d", __FILE__, __func__, __LINE__);
    return hxhim::Put(hx,
                      subject, subject_len,
                      predicate, predicate_len,
                      object_type, object, object_len);
}
