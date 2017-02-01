#ifndef HXHIM_TYPES_H
#define HXHIM_TYPES_H

#ifdef __cplusplus
extern "C"
{
/** Success constant */
#define HXHIM_OK = 0

/** Error constant */
#define HXHIM_ERROR = 1

/** HXHIM Option Values */
#define HXHIM_OPT_COMM_NULL (1 << 0)
#define HXHIM_OPT_COMM_MPI (1 << 1)
#define HXHIM_OPT_COMM_MERCURY (1 << 2)
#define HXHIM_OPT_STORE_NULL (1 << 8)
#define HXHIM_OPT_STORE_LEVELDB (1 << 9)
    
/**
 * Opaque struct that establishes the communication library in use
 */
typedef struct hxhim_cfg_comm_s hxhim_cfg_comm;

/**
 * Opaque struct that establishes the storage library to use
 */
typedef struct hxhim_cfg_store_s hxhim_cfg_store;

/**
 * HXHIM session data
 */
typedef struct hxhim_session {
    const hxhim_cfg_comm *comm;
    const hxhim_cfg_store *store;
} hxhim_session_t;
    
}
#endif // __cplusplus

#endif
