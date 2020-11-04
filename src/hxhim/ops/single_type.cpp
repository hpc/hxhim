#include "hxhim/single_type.hpp"
#include "hxhim/private/hxhim.hpp"
#include "utils/Blob.hpp"
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

    ::Stats::Chronostamp bput;
    bput.start = ::Stats::now();

    for(std::size_t i = 0; i < count; i++) {
        hxhim::PutImpl(hx,
                       hx->p->queues.puts.queue,
                       ReferenceBlob(subjects[i], subject_lens[i]),
                       ReferenceBlob(predicates[i], predicate_lens[i]),
                       object_type,
                       ReferenceBlob(objects[i], object_lens[i]));
    }

    #if ASYNC_PUTS
    hx->p->queues.puts.start_processing.notify_all();
    #else
    hxhim::serial_puts(hx);
    #endif

    mlog(HXHIM_CLIENT_DBG, "Completed %zu PUTs of type %d", count, object_type);
    bput.end = ::Stats::now();
    hx->p->stats.bulk_op[hxhim_op_t::HXHIM_PUT].emplace_back(bput);
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

    ::Stats::Chronostamp bget;
    bget.start = ::Stats::now();

    for(std::size_t i = 0; i < count; i++) {
        hxhim::GetImpl(hx,
                       hx->p->queues.gets,
                       ReferenceBlob(subjects[i], subject_lens[i]),
                       ReferenceBlob(predicates[i], predicate_lens[i]),
                       object_type);
    }

    mlog(HXHIM_CLIENT_DBG, "Completed %zu GETs of type %d", count, object_type);

    bget.end = ::Stats::now();
    hx->p->stats.bulk_op[hxhim_op_t::HXHIM_GET].emplace_back(bget);
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
                            std::size_t *num_records, enum hxhim_getop_t *ops,
                            const std::size_t count) {
    mlog(HXHIM_CLIENT_DBG, "Started %zu GETs of type %d", count, object_type);

    if (!valid(hx)   ||
        !subjects    || !subject_lens   ||
        !predicates  || !predicate_lens ||
        !num_records || ops)             {
        return HXHIM_ERROR;
    }

    ::Stats::Chronostamp bgetop;
    bgetop.start = ::Stats::now();

    for(std::size_t i = 0; i < count; i++) {
        hxhim::GetOpImpl(hx,
                         hx->p->queues.getops,
                         ReferenceBlob(subjects[i], subject_lens[i]),
                         ReferenceBlob(predicates[i], predicate_lens[i]),
                         object_type,
                         num_records[i], ops[i]);
    }

    mlog(HXHIM_CLIENT_DBG, "Completed %zu GETs of type %d", count, object_type);

    bgetop.end = ::Stats::now();
    hx->p->stats.bulk_op[hxhim_op_t::HXHIM_GETOP].emplace_back(bgetop);
    return HXHIM_SUCCESS;
}

int hxhimBGetOPSingleType(hxhim_t *hx,
                          void **subjects, size_t *subject_lens,
                          void **predicates, size_t *predicate_lens,
                          enum hxhim_object_type_t object_type,
                          size_t *num_records, enum hxhim_getop_t *ops,
                          const size_t count) {
    return hxhim::BGetOpSingleType(hx,
                                   subjects, subject_lens,
                                   predicates, predicate_lens,
                                   object_type,
                                   num_records, ops,
                                   count);
}
