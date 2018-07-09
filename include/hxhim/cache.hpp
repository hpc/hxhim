#ifndef HXHIM_CACHE
#define HXHIM_CACHE

#include <condition_variable>
#include <mutex>
#include <type_traits>

#include "constants.h"
#include "get_op.h"

namespace hxhim {
    typedef struct SubjectPredicate {
        void *subjects[HXHIM_MAX_BULK_OPS];
        std::size_t subject_lens[HXHIM_MAX_BULK_OPS];
        void *predicates[HXHIM_MAX_BULK_OPS];
        std::size_t predicate_lens[HXHIM_MAX_BULK_OPS];
    } SP_t;

    typedef struct SubjectPredicateObject : SP_t {
        void *objects[HXHIM_MAX_BULK_OPS];
        std::size_t object_lens[HXHIM_MAX_BULK_OPS];
    } SPO_t;

    struct UnsafeOp {
        int databases[HXHIM_MAX_BULK_OPS];
    };

    struct PutData : SPO_t {
        PutData *prev;
        PutData *next;
    };

    struct GetData : SP_t {
        GetData *next;
    };

    struct GetOpData : SP_t {
        std::size_t counts[HXHIM_MAX_BULK_GET_OPS];
        hxhim_get_op ops[HXHIM_MAX_BULK_GET_OPS];
        GetOpData *next;
    };

    struct DeleteData : SP_t {
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

    template <typename Data, typename = std::is_base_of<SubjectPredicate, Data> >
    void clean(Data *node) {
        while (node) {
            Data *next = node->next;
            delete node;
            node = next;
        }
    }
}

#endif