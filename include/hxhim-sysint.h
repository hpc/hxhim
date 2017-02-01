#ifndef HXHIM_SYSINT_H
#define HXHIM_SYSINT_H

#include <hxhim-types.h>

#ifdef __cplusplus
extern "C"
{
    /** */
    int hxhim_begin(hxhim_session* sess, int opts);

    /** */
    int hxhim_commit(hxhim_session* sess);

    /** */
    int hxhim_set_session(hxhim_session* sess);

    /** */
    hxhim_session* hxhim_get_session();

    /** */
    int hxhim_put(char* subject, char* predicate, char* object);

    /** */
    int hxhim_get(char* subject, char* predicate);

    /** */
    int hxhim_strerror(int hxhim_errnum);
}
#endif /* __cplusplus */

#endif
