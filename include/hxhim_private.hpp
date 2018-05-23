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
typedef struct key_stream {
    key_stream()
      : keys(), key_lens()
    {}

    // the mutex should be locked before calling clear
    virtual void clear() {
        keys.clear();
        key_lens.clear();
    }

    std::mutex mutex;
    std::deque<void *> keys;
    std::deque<std::size_t> key_lens;
} key_stream_t;

typedef struct unsafe_key_stream : public key_stream {
    unsafe_key_stream()
      : key_stream(),
        databases()
    {}

    void clear() {
        key_stream::clear();
        databases.clear();
    }

    std::deque<int> databases;
} unsafe_key_stream_t;

typedef struct keyvalue_stream : public key_stream {
    keyvalue_stream()
      : values(), value_lens()
    {}

    // the mutex should be locked before calling clear
    virtual void clear() {
        key_stream::clear();
        values.clear();
        value_lens.clear();
    }

    std::deque<void *> values;
    std::deque<std::size_t> value_lens;
} keyvalue_stream_t;

typedef struct unsafe_keyvalue_stream : public keyvalue_stream {
    unsafe_keyvalue_stream()
      : keyvalue_stream(),
        databases()
    {}

    void clear() {
        keyvalue_stream::clear();
        databases.clear();
    }

    std::deque<int> databases;
} unsafe_keyvalue_stream_t;

}

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct hxhim_private {
    mdhim_t *md;
    mdhim_options_t *mdhim_opts;

    // key value pairs are stored here until flush is called
    hxhim::keyvalue_stream_t puts;
    hxhim::key_stream_t gets;
    hxhim::key_stream dels;

    hxhim::unsafe_keyvalue_stream_t unsafe_puts;
    hxhim::unsafe_key_stream_t unsafe_gets;
    hxhim::unsafe_key_stream unsafe_dels;
} hxhim_private_t;

#ifdef __cplusplus
}
#endif

#endif
