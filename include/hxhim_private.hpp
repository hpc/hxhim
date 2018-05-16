#ifndef HXHIM_PRIVATE_HPP
#define HXHIM_PRIVATE_HPP

#include <list>
#include <mutex>
#include <vector>

#include "hxhim-types.h"
#include "mdhim.h"

namespace hxhim {

/**
 * Base operation structure
 */
typedef struct work {
    enum class Op : uint8_t {
        NONE,
        PUT,
        GET,
        DEL
    };

    work(const Op operation = Op::NONE)
      : op(operation),
        keys(), key_lens()
    {}

    virtual ~work(){}

    Op op;
    std::vector<void *> keys;
    std::vector<std::size_t> key_lens;
} work_t;

/**
 * Structure for put/bput operations
 */
typedef struct put_work : work_t {
    put_work()
      : work_t(Op::PUT),
        values(), value_lens()

    {}

    std::vector<void *> values;
    std::vector<std::size_t> value_lens;
} put_work_t;

/**
 * Structure for get/bget operations
 */
typedef struct get_work : work_t {
    get_work(const TransportGetMessageOp get_operation = TransportGetMessageOp::GET_EQ)
      : work_t(Op::GET),
        get_op(get_operation)
    {}

    TransportGetMessageOp get_op;
} get_work_t;

/**
 * Structure for del/bdel operations
 */
typedef struct del_work : work_t {
    del_work()
      : work_t(Op::DEL)
    {}
} del_work_t;

typedef std::list<work_t *> work_queue_t;

work_t *get_matching_work(hxhim_session_t *hx, const work_t::Op op);

}

typedef struct hxhim_session_private {
    mdhim_t *md;
    mdhim_options_t *mdhim_opts;

    std::mutex queue_mutex;
    hxhim::work_queue_t queue;
} hxhim_session_private_t;

#endif
