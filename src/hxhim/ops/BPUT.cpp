#include "hxhim/hxhim.hpp"
#include "hxhim/private/hxhim.hpp"
#include "utils/Blob.hpp"

/**
 * BPut
 * Add a BPUT into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param object_type   the type of the object
 * @param objects       the objects to put
 * @param object_lens   the lengths of the objects to put
 * @param count         the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BPut(hxhim_t *hx,
                void **subjects, std::size_t *subject_lens, enum hxhim_data_t *subject_types,
                void **predicates, std::size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                void **objects, std::size_t *object_lens, enum hxhim_data_t *object_types,
                const hxhim_put_permutation_t *permutations,
                const std::size_t count) {
    if (!valid(hx)  || !hx->p->running ||
        !subjects   || !subject_lens   || !subject_types   ||
        !predicates || !predicate_lens || !predicate_types ||
        !objects    || !object_lens    || !object_types    ||
        !permutations) {
        return HXHIM_ERROR;
    }

    ::Stats::Chronostamp bput;
    bput.start = ::Stats::now();

    // append these spo triples into the list of unsent PUTs
    for(std::size_t i = 0; i < count; i++) {
        hxhim::PutImpl(hx,
                       hx->p->queues.puts.queue,
                       ReferenceBlob(subjects[i], subject_lens[i], subject_types[i]),
                       ReferenceBlob(predicates[i], predicate_lens[i], predicate_types[i]),
                       ReferenceBlob(objects[i], object_lens[i], object_types[i]),
                       permutations[i]);
    }


    #if ASYNC_PUTS
    hx->p->queues.puts.start_processing.notify_all();
    #else
    hxhim::serial_puts(hx);
    #endif

    bput.end = ::Stats::now();
    hx->p->stats.bulk_op[hxhim_op_t::HXHIM_PUT].emplace_back(bput);
    return HXHIM_SUCCESS;
}

/**
 * hxhimBPut
 * Add a BPUT into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param object_types  the type of the object
 * @param objects       the objects to put
 * @param object_lens   the lengths of the objects to put
 * @param count         the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBPut(hxhim_t *hx,
              void **subjects, size_t *subject_lens, enum hxhim_data_t *subject_types,
              void **predicates, size_t *predicate_lens, enum hxhim_data_t *predicate_types,
              void **objects, size_t *object_lens, enum hxhim_data_t *object_types,
              const hxhim_put_permutation_t *permutations,
              const size_t count) {
    return hxhim::BPut(hx,
                       subjects, subject_lens, subject_types,
                       predicates, predicate_lens, predicate_types,
                       objects, object_lens, object_types,
                       permutations,
                       count);
}
