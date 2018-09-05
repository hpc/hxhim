#ifndef HXHIM_CACHE_HPP
#define HXHIM_CACHE_HPP

#include <condition_variable>
#include <mutex>
#include <type_traits>

#include "hxhim/constants.h"
#include "utils/FixedBufferPool.hpp"

namespace hxhim {
    typedef struct SubjectPredicate {
        SubjectPredicate(FixedBufferPool *arrays, const std::size_t count);
        virtual ~SubjectPredicate();

        FixedBufferPool *arrays;
        const std::size_t count;

        void **subjects;
        std::size_t *subject_lens;

        void **predicates;
        std::size_t *predicate_lens;
    } SP_t;

    typedef struct SubjectPredicateObject : SP_t {
        SubjectPredicateObject(FixedBufferPool *arrays, const std::size_t count);
        virtual ~SubjectPredicateObject();

        hxhim_type_t *object_types;
        void **objects;
        std::size_t *object_lens;
    } SPO_t;

    struct PutData : SPO_t {
        PutData(FixedBufferPool *arrays, const std::size_t count);

        PutData *prev;
        PutData *next;
    };

    struct GetData : SP_t {
        GetData(FixedBufferPool *arrays, const std::size_t count);
        ~GetData();

        hxhim_type_t *object_types;
        GetData *next;
    };

    struct GetOpData : SP_t {
        GetOpData(FixedBufferPool *arrays, const std::size_t count);
        ~GetOpData();

        hxhim_type_t *object_types;
        std::size_t *num_recs;
        hxhim_get_op_t *ops;
        GetOpData *next;
    };

    struct DeleteData : SP_t {
        DeleteData(FixedBufferPool *arrays, const std::size_t count);

        DeleteData *next;
    };

    template <typename Data, typename = std::is_base_of<SubjectPredicate, Data> >
    struct Unsent {
        std::mutex mutex;
        std::condition_variable start_processing;
        std::condition_variable done_processing;
        std::size_t full_batches;
        std::size_t last_count;
        Data *head;
        Data *tail;
        bool force;
    };
}

#endif
