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

    if (count) {
        hxhim::Unsent<hxhim::PutData> &puts = hx->p->queues.puts;
        std::lock_guard<std::mutex> lock(puts.mutex);

        // no previous batch
        if (!puts.tail) {
            puts.head       = hx->p->memory_pools.bulks->acquire<hxhim::PutData>(hx->p->memory_pools.arrays, hx->p->max_bulk_ops.puts);
            puts.tail       = puts.head;
            puts.last_count = 0;
        }

        for(std::size_t c = 0; c < count; c++) {
            // filled the current batch
            if (puts.last_count == HXHIM_MAX_BULK_PUT_OPS) {
                hxhim::PutData *next = hx->p->memory_pools.bulks->acquire<hxhim::PutData>(hx->p->memory_pools.arrays, hx->p->max_bulk_ops.puts);
                next->prev      = puts.tail;
                puts.tail->next = next;
                puts.tail       = next;
                puts.last_count = 0;
                puts.full_batches++;
                puts.start_processing.notify_one();
            }

            std::size_t &i = puts.last_count;
            puts.tail->subjects[i] = subjects[c];
            puts.tail->subject_lens[i] = subject_lens[c];

            puts.tail->predicates[i] = predicates[c];
            puts.tail->predicate_lens[i] = predicate_lens[c];

            puts.tail->object_types[i] = object_type;
            puts.tail->objects[i] = objects[c];
            puts.tail->object_lens[i] = object_lens[c];
            i++;
        }
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

    if (count) {
        hxhim::Unsent<hxhim::GetData> &gets = hx->p->queues.gets;
        std::lock_guard<std::mutex> lock(gets.mutex);

        // no previous batch
        if (!gets.tail) {
            gets.head       = hx->p->memory_pools.bulks->acquire<hxhim::GetData>(hx->p->memory_pools.arrays, hx->p->max_bulk_ops.gets);
            gets.tail       = gets.head;
            gets.last_count = 0;
        }

        for(std::size_t c = 0; c < count; c++) {
            // filled the current batch
            if (gets.last_count == HXHIM_MAX_BULK_GET_OPS) {
                gets.tail->next = hx->p->memory_pools.bulks->acquire<hxhim::GetData>(hx->p->memory_pools.arrays, hx->p->max_bulk_ops.gets);
                gets.tail       = gets.tail->next;
                gets.last_count = 0;
                gets.full_batches++;
            }

            std::size_t &i = gets.last_count;

            gets.tail->subjects[i] = subjects[c];
            gets.tail->subject_lens[i] = subject_lens[c];

            gets.tail->predicates[i] = predicates[c];
            gets.tail->predicate_lens[i] = predicate_lens[c];

            gets.tail->object_types[i] = object_type;

            i++;
        }
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
