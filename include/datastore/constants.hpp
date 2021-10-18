#ifndef DATASTORE_CONSTANTS_HPP
#define DATASTORE_CONSTANTS_HPP

#define DATASTORE_SUCCESS 1
#define DATASTORE_ERROR   2
#define DATASTORE_UNSET   3

namespace Datastore {

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

#endif
