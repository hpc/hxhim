#ifndef HXHIM_PRIVATE_HPP
#define HXHIM_PRIVATE_HPP

#include <deque>
#include <mutex>

#include "hxhim-types.h"
#include "hxhim_work_op.h"
#include "mdhim.hpp"

namespace hxhim {

/**
 * Base operation structure
 */
typedef struct put_stream {
    put_stream()
      : keys(), key_lens(),
        values(), value_lens()
    {}

    // the mutex should be locked before calling clear
    void clear() {
        keys.clear();
        key_lens.clear();
        values.clear();
        value_lens.clear();
    }

    std::mutex mutex;
    std::deque<void *> keys;
    std::deque<std::size_t> key_lens;
    std::deque<void *> values;
    std::deque<std::size_t> value_lens;
} put_stream_t;

}

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct hxhim_private {
    mdhim_t *md;
    mdhim_options_t *mdhim_opts;

    // key value pairs are stored here until flush is called
    hxhim::put_stream_t puts;
} hxhim_private_t;

#ifdef __cplusplus
}
#endif

#endif
