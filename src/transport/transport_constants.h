#ifndef HXHIM_TRANSPORT_CONSTANTS
#define HXHIM_TRANSPORT_CONSTANTS

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * TransportMessageType
 * List of available message types
 */
enum TransportMessageType {
    INVALID   = 0,
    PUT       = 1,
    BPUT      = 2,
    BGET      = 3,
    DELETE    = 4,
    BDELETE   = 5,
    CLOSE     = 6,
    RECV      = 7,
    RECV_GET  = 8,
    RECV_BGET = 9,
    COMMIT    = 10
};

/**
 * TransportGetMessageOp
 * List of operations that can be done
 * with a Get or BGet
 */
enum TransportGetMessageOp {
    GET_EQ         = 0,
    GET_NEXT       = 1,
    GET_PREV       = 2,
    GET_FIRST      = 3,
    GET_LAST       = 4,
    GET_PRIMARY_EQ = 5,

    GET_OP_MAX     = 255
};

#ifdef __cplusplus
}
#endif

#endif
