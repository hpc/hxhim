#include "hxhim/single_type.hpp"
#include "hxhim/private/hxhim.hpp"
#include "utils/Blob.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

int hxhim::BPutSingleType(hxhim_t *hx,
                          void **subjects, std::size_t *subject_lens,
                          void **predicates, std::size_t *predicate_lens,
                          enum hxhim_object_type_t object_type, void **objects, std::size_t *object_lens,
                          const std::size_t count) {

    mlog(HXHIM_CLIENT_DBG, "Started %zu PUTs of type %d", count, object_type);

    if (!valid(hx)  ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens ||
        !objects    || !object_lens)    {
        return HXHIM_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        hxhim::PutImpl(hx->p->queues.puts,
                       ReferenceBlob(subjects[i], subject_lens[i]),
                       ReferenceBlob(predicates[i], predicate_lens[i]),
                       object_type,
                       ReferenceBlob(objects[i], object_lens[i]));
    }

    mlog(HXHIM_CLIENT_DBG, "Completed %zu PUTs of type %d", count, object_type);
    return HXHIM_SUCCESS;
}

int hxhimBPutSingleType(hxhim_t *hx,
                        void **subjects, size_t *subject_lens,
                        void **predicates, size_t *predicate_lens,
                        enum hxhim_object_type_t object_type, void **objects, size_t *object_lens,
                        const size_t count) {
    return hxhim::BPutSingleType(hx,
                                 subjects, subject_lens,
                                 predicates, predicate_lens,
                                 object_type, objects, object_lens,
                                 count);
}

int hxhim::BGetSingleType(hxhim_t *hx,
                          void **subjects, std::size_t *subject_lens,
                          void **predicates, std::size_t *predicate_lens,
                          enum hxhim_object_type_t object_type,
                          const std::size_t count) {
    mlog(HXHIM_CLIENT_DBG, "Started %zu GETs of type %d", count, object_type);

    if (!valid(hx)  ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens) {
        return HXHIM_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        hxhim::GetImpl(hx->p->queues.gets,
                       ReferenceBlob(subjects[i], subject_lens[i]),
                       ReferenceBlob(predicates[i], predicate_lens[i]),
                       object_type);
    }

    mlog(HXHIM_CLIENT_DBG, "Completed %zu GETs of type %d", count, object_type);
    return HXHIM_SUCCESS;
}

int hxhimBGetSingleType(hxhim_t *hx,
                        void **subjects, size_t *subject_lens,
                        void **predicates, size_t *predicate_lens,
                        enum hxhim_object_type_t object_type,
                        const size_t count) {
    return hxhim::BGetSingleType(hx,
                                 subjects, subject_lens,
                                 predicates, predicate_lens,
                                 object_type,
                                 count);
}

int hxhim::BGetOpSingleType(hxhim_t *hx,
                            void **subjects, std::size_t *subject_lens,
                            void **predicates, std::size_t *predicate_lens,
                            enum hxhim_object_type_t object_type,
                            std::size_t *num_records, enum hxhim_get_op_t *ops,
                            const std::size_t count) {
    mlog(HXHIM_CLIENT_DBG, "Started %zu GETs of type %d", count, object_type);

    if (!valid(hx)   ||
        !subjects    || !subject_lens   ||
        !predicates  || !predicate_lens ||
        !num_records || ops)             {
        return HXHIM_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        hxhim::GetOpImpl(hx->p->queues.getops,
                         ReferenceBlob(subjects[i], subject_lens[i]),
                         ReferenceBlob(predicates[i], predicate_lens[i]),
                         object_type,
                         num_records[i], ops[i]);
    }

    mlog(HXHIM_CLIENT_DBG, "Completed %zu GETs of type %d", count, object_type);
    return HXHIM_SUCCESS;
}

int hxhimBGetOPSingleType(hxhim_t *hx,
                          void **subjects, size_t *subject_lens,
                          void **predicates, size_t *predicate_lens,
                          enum hxhim_object_type_t object_type,
                          size_t *num_records, enum hxhim_get_op_t *ops,
                          const size_t count) {
    return hxhim::BGetOpSingleType(hx,
                                   subjects, subject_lens,
                                   predicates, predicate_lens,
                                   object_type,
                                   num_records, ops,
                                   count);
}
