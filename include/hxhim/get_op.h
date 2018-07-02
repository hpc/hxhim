#ifndef HXHIM_GET_OP
#define HXHIM_GET_OP

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * GetOp
 * List of operations that can be done
 * with a Get or BGet
 */
enum hxhim_get_op {
    HXHIM_GET_EQ         = 0,
    HXHIM_GET_NEXT       = 1,
    HXHIM_GET_PREV       = 2,
    HXHIM_GET_FIRST      = 3,
    HXHIM_GET_LAST       = 4,
    HXHIM_GET_PRIMARY_EQ = 5,

    HXHIM_GET_OP_MAX     = 255
};


#ifdef __cplusplus
}
#endif

#endif
