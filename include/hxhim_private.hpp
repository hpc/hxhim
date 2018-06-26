#ifndef HXHIM_PRIVATE_HPP
#define HXHIM_PRIVATE_HPP

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <type_traits>

#include "abt.h"

#include "hxhim-types.h"
#include "hxhim_work_op.h"
#include "mdhim.hpp"
#include "return.hpp"

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

    class Return;
}

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct hxhim_private {
    mdhim_t *md;
    mdhim_options_t *mdhim_opts;

    std::atomic_bool running;

    std::thread thread;
    // ABT_xstream stream;
    // ABT_pool pool;
    // ABT_thread thread;

    std::size_t watermark;
    hxhim::Unsent<hxhim::PutData> puts;
    hxhim::Unsent<hxhim::GetData> gets;
    hxhim::Unsent<hxhim::GetOpData> getops;
    hxhim::Unsent<hxhim::DeleteData> deletes;

    hxhim::Unsent<hxhim::UnsafePutData> unsafe_puts;
    hxhim::Unsent<hxhim::UnsafeGetData> unsafe_gets;
    hxhim::Unsent<hxhim::UnsafeGetOpData> unsafe_getops;
    hxhim::Unsent<hxhim::UnsafeDeleteData> unsafe_deletes;

    std::mutex results_mutex;
    hxhim::Return *results;

} hxhim_private_t;

#ifdef __cplusplus
}
#endif

#endif
