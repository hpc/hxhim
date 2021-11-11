#include "hxhim/hxhim.hpp"
#include "hxhim/private/hxhim.hpp"
#include "utils/Blob.hpp"

/**
 * BGet
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param hx             the HXHIM session
 * @param subjects       the subjects to get
 * @param subject_lens   the lengths of the subjects to get
 * @param predicates     the prediates to get
 * @param predicate_lens the lengths of the prediates to get
 * @param object_types   the types of the objects
 * @param count          the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BGet(hxhim_t *hx,
                void **subjects, std::size_t *subject_lens, enum hxhim_data_t *subject_types,
                void **predicates, std::size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                hxhim_data_t *object_types,
                const std::size_t count) {
    if (!started(hx) ||
        !subjects    || !subject_lens   || !subject_types   ||
        !predicates  || !predicate_lens || !predicate_types ||
        !object_types) {
        return HXHIM_ERROR;
    }

    ::Stats::Chronostamp bget;
    bget.start = ::Stats::now();

    for(std::size_t i = 0; i < count; i++) {
        hxhim::GetImpl(hx,
                       hx->p->queues.gets,
                       ReferenceBlob(subjects[i], subject_lens[i], subject_types[i]),
                       ReferenceBlob(predicates[i], predicate_lens[i], predicate_types[i]),
                       object_types[i]);
    }

    bget.end = ::Stats::now();
    hx->p->stats.bulk_op[hxhim_op_t::HXHIM_GET].emplace_back(bget);
    return HXHIM_SUCCESS;
}

/**
 * hxhimBGet
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subjects       the subjects to get
 * @param subject_lens   the lengths of the subjects to get
 * @param predicates     the prediates to get
 * @param predicate_lens the lengths of the prediates to get
 * @param object_types   the types of the objects
 * @param objects        the prediates to get
 * @param object_lens    the lengths of the prediates to get
 * @param count          the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBGet(hxhim_t *hx,
              void **subjects, size_t *subject_lens, enum hxhim_data_t *subject_types,
              void **predicates, size_t *predicate_lens, enum hxhim_data_t *predicate_types,
              enum hxhim_data_t *object_types,
              const std::size_t count) {
    return hxhim::BGet(hx,
                       subjects, subject_lens, subject_types,
                       predicates, predicate_lens, predicate_types,
                       object_types,
                       count);
}
