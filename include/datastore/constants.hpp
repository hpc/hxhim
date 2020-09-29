#ifndef HXHIM_DATASTORE_CONSTANTS_HPP
#define HXHIM_DATASTORE_CONSTANTS_HPP

#define DATASTORE_SUCCESS 1
#define DATASTORE_ERROR   2

namespace hxhim {
namespace datastore {

/**
 * hxhim_datastore_t
 * The types of datastores that are available
 */
enum Type {
    IN_MEMORY,
    #if HXHIM_HAVE_LEVELDB
    LEVELDB,
    #endif
    #if HXHIM_HAVE_ROCKSDB
    ROCKSDB,
    #endif
};

}
}

#endif
