#include "hxhim/hxhim.hpp"
#include "hxhim/private/hxhim.hpp"
#include "utils/Blob.hpp"

/**
 * BGetOp
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subjects       the subjects to get
 * @param subject_lens   the lengths of the subjects to get
 * @param predicates     the predicates to get
 * @param predicate_lens the lengths of the predicates to get
 * @param object_types   the type of the objects
 * @param num_records    maximum numbers of records to GET
 * @param op             the operations to use
 * @param count          the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BGetOp(hxhim_t *hx,
                  void **subjects, std::size_t *subject_lens, enum hxhim_data_t *subject_types,
                  void **predicates, std::size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                  enum hxhim_data_t *object_types,
                  std::size_t *num_records, enum hxhim_getop_t *ops,
                  const std::size_t count) {
    if (!started(hx)  ||
        !subjects     || !subject_lens   || !subject_types   ||
        !predicates   || !predicate_lens || !predicate_types ||
        !object_types ||
        !num_records  || !ops) {
        return HXHIM_ERROR;
    }

    ::Stats::Chronostamp bgetop;
    bgetop.start = ::Stats::now();

    for(std::size_t i = 0; i < count; i++) {
        hxhim::GetOpImpl(hx,
                         hx->p->queues.getops,
                         ReferenceBlob(subjects[i], subject_lens[i], subject_types[i]),
                         ReferenceBlob(predicates[i], predicate_lens[i], predicate_types[i]),
                         object_types[i],
                         num_records[i], ops[i]);
    }

    bgetop.end = ::Stats::now();
    hx->p->stats.bulk_op[hxhim_op_t::HXHIM_GETOP].emplace_back(bgetop);
    return HXHIM_SUCCESS;
}

/**
 * hxhimBGetOp
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subjects       the subjects to get
 * @param subject_lens   the lengths of the subjects to get
 * @param predicates     the predicates to get
 * @param predicate_lens the lengths of the predicates to get
 * @param object_types   the type of the objects
 * @param num_records    maximum numbers of records to GET
 * @param op             the operations to use
 * @param count          the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBGetOp(hxhim_t *hx,
                void **subjects, size_t *subject_lens, enum hxhim_data_t *subject_types,
                void **predicates, size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                enum hxhim_data_t *object_types,
                size_t *num_records, enum hxhim_getop_t *ops,
                const size_t count) {
    return hxhim::BGetOp(hx,
                         subjects, subject_lens, subject_types,
                         predicates, predicate_lens, predicate_types,
                         object_types,
                         num_records, ops,
                         count);
}
