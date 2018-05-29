#ifndef HXHIM_PRIVATE_HPP
#define HXHIM_PRIVATE_HPP

#include <list>
#include <mutex>

#include "hxhim-types.h"
#include "hxhim_work_op.h"
#include "mdhim.hpp"

namespace hxhim {

/**
 * Structures for storing data for passing into MDHIM
 */
typedef struct subject_predicate {
    void *subject;
    std::size_t subject_len;
    void *predicate;
    std::size_t predicate_len;
} sp_t;

typedef struct subject_predicate_object : sp_t {
    void *object;
    std::size_t object_len;
} spo_t;

typedef struct unsafe_subject_predicate : sp_t {
    int database;
} unsafe_sp_t;

typedef struct unsafe_subject_predicate_object : spo_t {
    int database;
} unsafe_spo_t;

typedef struct subject_predicate_op : sp_t {
    enum TransportGetMessageOp op;
    std::size_t num_records;
} sp_op_t;

typedef struct unsafe_subject_predicate_op : sp_op_t {
    int database;
} unsafe_sp_op_t;

}

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct hxhim_private {
    mdhim_t *md;
    mdhim_options_t *mdhim_opts;

    // data is stored here until flush is called
    struct {
        std::mutex mutex;
        std::list<hxhim::spo_t> data;
    } puts;

    struct {
        std::mutex mutex;
        std::list<hxhim::sp_t> data;
    } gets;

    struct {
        std::mutex mutex;
        std::list<hxhim::sp_op_t> data;
    } getops;

    struct {
        std::mutex mutex;
        std::list<hxhim::sp_t>  data;
    } dels;

    struct {
        std::mutex mutex;
        std::list<hxhim::unsafe_spo_t>  data;
    } unsafe_puts;

    struct {
        std::mutex mutex;
        std::list<hxhim::unsafe_sp_t>  data;
    } unsafe_gets;

    struct {
        std::mutex mutex;
        std::list<hxhim::unsafe_sp_op_t>  data;
    } unsafe_getops;

    struct {
        std::mutex mutex;
        std::list<hxhim::unsafe_sp_t>  data;
    } unsafe_dels;
} hxhim_private_t;

#ifdef __cplusplus
}
#endif

#endif
