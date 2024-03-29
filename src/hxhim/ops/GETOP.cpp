#include "hxhim/hxhim.hpp"
#include "hxhim/private/hxhim.hpp"
#include "utils/Blob.hpp"

/**
 * GetOp
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subject        the subjects to get
 * @param subject_len    the lengths of the subjects to get
 * @param predicate      the predicates to get
 * @param predicate_len  the lengths of the predicates to get
 * @param object_type    the type of the object
 * @param num_records    maximum number of records to GET
 * @param op             the operation to use
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::GetOp(hxhim_t *hx,
                 void *subject, std::size_t subject_len, enum hxhim_data_t subject_type,
                 void *predicate, std::size_t predicate_len, enum hxhim_data_t predicate_type,
                 enum hxhim_data_t object_type,
                 std::size_t num_records, enum hxhim_getop_t op) {
    // NULL predicates are allowed
    if (!started(hx) ||
        !subject     || !subject_len) {
        return HXHIM_ERROR;
    }

    ::Stats::Chronostamp bgetop;
    bgetop.start = ::Stats::now();
    const int rc = hxhim::GetOpImpl(hx,
                                    hx->p->queues.getops,
                                    ReferenceBlob(subject, subject_len, subject_type),
                                    ReferenceBlob(predicate, predicate_len, predicate_type),
                                    object_type,
                                    num_records, op);
    bgetop.end = ::Stats::now();
    hx->p->stats.bulk_op[hxhim_op_t::HXHIM_GETOP].emplace_back(bgetop);
    return rc;
}

/**
 * hxhimGetOp
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subject        the subjects to get
 * @param subject_len    the lengths of the subjects to get
 * @param predicate      the predicates to get
 * @param predicate_len  the lengths of the predicates to get
 * @param object_type    the type of the object
 * @param num_records    maximum number of records to GET
 * @param op             the operation to use
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimGetOp(hxhim_t *hx,
               void *subject, size_t subject_len, enum hxhim_data_t subject_type,
               void *predicate, size_t predicate_len, enum hxhim_data_t predicate_type,
               enum hxhim_data_t object_type,
               size_t num_records, enum hxhim_getop_t op) {
    return hxhim::GetOp(hx,
                        subject, subject_len, subject_type,
                        predicate, predicate_len, predicate_type,
                        object_type,
                        num_records, op);
}
