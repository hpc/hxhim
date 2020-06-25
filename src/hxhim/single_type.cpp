#include "hxhim/single_type.h"
#include "hxhim/single_type.hpp"
#include "hxhim/private.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

int hxhim::BPutSingleType(hxhim_t *hx,
                          void **subjects, std::size_t *subject_lens,
                          void **predicates, std::size_t *predicate_lens,
                          enum hxhim_type_t object_type, void **objects, std::size_t *object_lens,
                          std::size_t count) {

    mlog(HXHIM_CLIENT_DBG, "Started %zu PUTs of type %d", count, object_type);

    if (!valid(hx)  ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens ||
        !objects    || !object_lens)    {
        return HXHIM_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        hxhim::PutImpl(hx->p->queues.puts, subjects[i], subject_lens[i], predicates[i], predicate_lens[i], object_type, objects[i], object_lens[i]);
    }

    mlog(HXHIM_CLIENT_DBG, "Completed %zu PUTs of type %d", count, object_type);
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
    mlog(HXHIM_CLIENT_DBG, "Started %zu GETs of type %d", count, object_type);

    if (!valid(hx)  ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens) {
        return HXHIM_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        hxhim::GetImpl(hx->p->queues.gets, subjects[i], subject_lens[i], predicates[i], predicate_lens[i], object_type);
    }

    mlog(HXHIM_CLIENT_DBG, "Completed %zu GETs of type %d", count, object_type);
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
