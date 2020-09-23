#ifndef HXHIM_DATASTORES_HPP
#define HXHIM_DATASTORES_HPP

#include "datastore/datastore.hpp"
#include "datastore/InMemory.hpp"

#if HXHIM_HAVE_LEVELDB
#include "datastore/leveldb.hpp"
#endif

#if HXHIM_HAVE_ROCKSDB
#include "datastore/rocksdb.hpp"
#endif

#include "hxhim/struct.h"

namespace hxhim {
namespace datastore {

int Init(hxhim_t *hx,
         const int offset,
         Config *config,
         const Histogram::Config &hist_config,
         const std::string *exact_name = nullptr);

int destroy(hxhim_t *hx);

}
}

#endif
