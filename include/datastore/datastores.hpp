#ifndef HXHIM_DATASTORES_HPP
#define HXHIM_DATASTORES_HPP

#include "datastore/datastore.hpp"
#include "datastore/transform.hpp"
#include "datastore/InMemory.hpp"

#if HXHIM_HAVE_LEVELDB
#include "datastore/LevelDB.hpp"
#endif

#if HXHIM_HAVE_ROCKSDB
#include "datastore/RocksDB.hpp"
#endif

#include "hxhim/struct.h"

namespace Datastore {

int Init(hxhim_t *hx,
         const int id,
         Config *config,
         Transform::Callbacks *callbacks,
         const Histogram::Config &hist_config,
         const std::string *exact_name, // full path + name of datastore; ignores config
         const bool do_open,
         const bool read_histograms,
         const bool write_histograms);

int destroy(hxhim_t *hx);

}

#endif
