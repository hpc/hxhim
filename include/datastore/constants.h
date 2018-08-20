#ifndef HXHIM_DATASTORE_CONSTANTS_H
#define HXHIM_DATASTORE_CONSTANTS_H

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * hxhim_datastore_t
 * The types of datastores that are available
 */
enum hxhim_datastore_t {
    #if HXHIM_HAVE_LEVELDB
    HXHIM_DATASTORE_LEVELDB,
    #endif
    HXHIM_DATASTORE_IN_MEMORY,
};

#ifdef __cplusplus
}
#endif

#endif
