#ifndef HXHIM_PRIVATE_HPP
#define HXHIM_PRIVATE_HPP

#include <mutex>
#include <type_traits>

#include "hxhim-types.h"
#include "hxhim_work_op.h"
#include "mdhim.hpp"

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
        PutData *next;
    };

    struct GetData : SP_t {
        GetData *next;
    };

    struct GetOpData : SP_t {
        TransportGetMessageOp ops[HXHIM_MAX_BULK_GET_OPS];
        GetOpData *next;
    };

    struct DeleteData : SP_t {
        DeleteData *next;
    };

    struct UnsafePutData : SPO_t, UnsafeOp {
        UnsafePutData *next;
    };

    struct UnsafeGetData : SP_t, UnsafeOp {
        UnsafeGetData *next;
    };

    struct UnsafeGetOpData : SP_t, UnsafeOp {
        TransportGetMessageOp ops[HXHIM_MAX_BULK_GET_OPS];
        UnsafeGetOpData *next;
    };

    struct UnsafeDeleteData : SP_t, UnsafeOp {
        UnsafeDeleteData *next;
    };

    template <typename Data, typename = std::is_base_of<SubjectPredicate, Data> >
    struct Batch {
        std::mutex mutex;
        std::size_t last_count;
        Data *head;
        Data *tail;
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

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct hxhim_private {
    mdhim_t *md;
    mdhim_options_t *mdhim_opts;

    hxhim::Batch<hxhim::PutData> puts;
    hxhim::Batch<hxhim::GetData> gets;
    hxhim::Batch<hxhim::GetOpData> getops;
    hxhim::Batch<hxhim::DeleteData> deletes;

    hxhim::Batch<hxhim::UnsafePutData> unsafe_puts;
    hxhim::Batch<hxhim::UnsafeGetData> unsafe_gets;
    hxhim::Batch<hxhim::UnsafeGetOpData> unsafe_getops;
    hxhim::Batch<hxhim::UnsafeDeleteData> unsafe_deletes;
} hxhim_private_t;

#ifdef __cplusplus
}
#endif

#endif
