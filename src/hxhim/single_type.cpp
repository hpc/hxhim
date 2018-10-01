#include "hxhim/single_type.h"
#include "hxhim/single_type.hpp"
#include "hxhim/private.hpp"

int hxhim::BPutSingleType(hxhim_t *hx,
                          void **subjects, std::size_t *subject_lens,
                          void **predicates, std::size_t *predicate_lens,
                          enum hxhim_type_t object_type, void **objects, std::size_t *object_lens,
                          std::size_t count) {
    if (!hx         || !hx->p          ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens ||
        !objects    || !object_lens)    {
        return HXHIM_ERROR;
    }

    hxhim::Unsent<hxhim::PutData> &puts = hx->p->queues.puts;
    std::lock_guard<std::mutex> lock(puts.mutex);

    for(std::size_t i = 0; i < count; i++) {
        hxhim::PutImpl(hx, subjects[i], subject_lens[i], predicates[i], predicate_lens[i], object_type, objects[i], object_lens[i]);
    }

    return HXHIM_SUCCESS;
}

int hxhimBPutSingleType(hxhim_t *hx,
                        void **subjects, std::size_t *subject_lens,
                        void **predicates, std::size_t *predicate_lens,
                        enum hxhim_type_t object_type, void **objects, std::size_t *object_lens,
                        std::size_t count) {
    return hxhim::BPutSingleType(hx,
                                 subjects, subject_lens,
                                 predicates, predicate_lens,
                                 object_type, objects, object_lens,
                                 count);
}

int hxhim::BGetSingleType(hxhim_t *hx,
                          void **subjects, std::size_t *subject_lens,
                          void **predicates, std::size_t *predicate_lens,
                          enum hxhim_type_t object_type,
                          std::size_t count) {
    if (!hx         || !hx->p          ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens) {
        return HXHIM_ERROR;
    }

    hxhim::Unsent<hxhim::GetData> &gets = hx->p->queues.gets;
    std::lock_guard<std::mutex> lock(gets.mutex);

    for(std::size_t i = 0; i < count; i++) {
        hxhim::GetImpl(hx, subjects[i], subject_lens[i], predicates[i], predicate_lens[i], object_type);
    }

    return HXHIM_SUCCESS;
}

int hxhimBGetSingleType(hxhim_t *hx,
                        void **subjects, std::size_t *subject_lens,
                        void **predicates, std::size_t *predicate_lens,
                        enum hxhim_type_t object_type,
                        std::size_t count) {
    return hxhim::BGetSingleType(hx,
                                 subjects, subject_lens,
                                 predicates, predicate_lens,
                                 object_type,
                                 count);
}
