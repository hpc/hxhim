#include "hxhim/hxhim.hpp"
#include "hxhim/private/hxhim.hpp"
#include "utils/Blob.hpp"

/**
 * BDelete
 * Add a BDEL into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to delete
 * @param subject_lens  the lengths of the subjects to delete
 * @param prediates     the prediates to delete
 * @param prediate_lens the lengths of the prediates to delete
 * @param object_type   the type of the object
 * @param count         the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BDelete(hxhim_t *hx,
                   void **subjects, std::size_t *subject_lens, enum hxhim_data_t *subject_types,
                   void **predicates, std::size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                   const std::size_t count) {
    if (!started(hx) ||
        !subjects    || !subject_lens   || !subject_types   ||
        !predicates  || !predicate_lens || !predicate_types) {
        return HXHIM_ERROR;
    }

    ::Stats::Chronostamp bdel;
    bdel.start = ::Stats::now();

    for(std::size_t i = 0; i < count; i++) {
        hxhim::DeleteImpl(hx,
                          hx->p->queues.deletes,
                          ReferenceBlob(subjects[i], subject_lens[i], subject_types[i]),
                          ReferenceBlob(predicates[i], predicate_lens[i], predicate_types[i]));
    }

    bdel.end = ::Stats::now();
    hx->p->stats.bulk_op[hxhim_op_t::HXHIM_DELETE].emplace_back(bdel);
    return HXHIM_SUCCESS;
}

/**
 * hxhimBDelete
 * Add a BDEL into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to delete
 * @param subject_lens  the lengths of the subjects to delete
 * @param prediates     the prediates to delete
 * @param prediate_lens the lengths of the prediates to delete
 * @param object_type   the type of the object
 * @param count         the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBDelete(hxhim_t *hx,
                 void **subjects, size_t *subject_lens, enum hxhim_data_t *subject_types,
                 void **predicates, size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                 const size_t count) {
    return hxhim::BDelete(hx,
                          subjects, subject_lens, subject_types,
                          predicates, predicate_lens, predicate_types,
                          count);
}
