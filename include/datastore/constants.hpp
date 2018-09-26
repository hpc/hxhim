#ifndef HXHIM_DATASTORE_CONSTANTS_HPP
#define HXHIM_DATASTORE_CONSTANTS_HPP

namespace hxhim {
namespace datastore {

/**
 * hxhim_datastore_t
 * The types of datastores that are available
 */
enum Type {
    #if HXHIM_HAVE_LEVELDB
    LEVELDB,
    #endif
    IN_MEMORY,
};

}
}

#endif
