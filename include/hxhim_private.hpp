#ifndef HXHIM_PRIVATE_HPP
#define HXHIM_PRIVATE_HPP

#include <list>
#include <mutex>
#include <vector>

#include "hxhim-types.h"
#include "mdhim.h"

typedef struct hxhim_work {
    enum class Op : uint8_t {
        NONE,
        PUT,
        GET,
        DEL
    };

    hxhim_work(const Op operation = Op::NONE)
      : op(operation),
        keys(), key_lens()
    {}

    virtual ~hxhim_work(){}

    Op op;
    std::vector<void *> keys;
    std::vector<std::size_t> key_lens;
} hxhim_work_t;

typedef struct hxhim_put_work : hxhim_work_t {
    hxhim_put_work()
      : hxhim_work_t(Op::PUT),
        values(), value_lens()

    {}

    std::vector<void *> values;
    std::vector<std::size_t> value_lens;
} hxhim_put_work_t;

typedef struct hxhim_get_work : hxhim_work_t {
    hxhim_get_work(const TransportGetMessageOp get_operation = TransportGetMessageOp::GET_EQ)
      : hxhim_work_t(Op::GET),
        get_op(get_operation)
    {}

    TransportGetMessageOp get_op;
} hxhim_get_work_t;

typedef struct hxhim_del_work : hxhim_work_t {
    hxhim_del_work()
      : hxhim_work_t(Op::DEL)
    {}
} hxhim_del_work_t;

typedef std::list<hxhim_work_t *> hxhim_work_queue_t;

typedef struct hxhim_session_private {
    mdhim_t *md;
    mdhim_options_t *mdhim_opts;

    std::mutex queue_mutex;
    hxhim_work_queue_t queue;
} hxhim_session_private_t;

hxhim_work_t *get_matching_work(hxhim_session_t *hx, const hxhim_work_t::Op op);

#endif
