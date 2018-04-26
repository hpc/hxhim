#ifndef HXHIM_SYSINT_H
#define HXHIM_SYSINT_H

#include <hxhim-types.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C"
{
#endif
    
    /**
     * @description Begin an HXHIM session
     * @param sess a pointer to an uninitialized session
     * @param opts the options for this session
     * @return 0 on success, non-zero on failure
     */
    int hxhim_sess_create(hxhim_session_t* sess, int opts);

    /**
     * @description Ensure the session is stored to stable storage.
     * @param sess the session to persist
     */
    int hxhim_sess_sync(hxhim_session_t* sess);
    
    /**
     * @description Transportit an HXHIM session. Data is not persistent until
     *  commit is called. This call blocks until all data is persistent.
     * @param sess a pointer to to a session
     * @return 0 if the RDF triples are successfully stored
     */
    int hxhim_sess_wait(hxhim_session_t* sess);

    /**
     * @description Change the active HXHIM session
     * @param Set the active session
     * @return 0
     */
    int hxhim_sess(hxhim_session_t* sess);

    /**
     * @description Retrieve the currect HXHIM session
     * @return the active session
     */
    hxhim_session_t* hxhim_get_session();

    /**
     * @description Register an RDF triple with the session
     * @param subject A pointer to a null terminated subject
     * @param predicate A pointer to a null terminated predicate
     * @param object A pointer to a varying length object
     * @param objsize The object size
     */
    int hxhim_put(char* subject, char* predicate, void* object, size_t objsize);

    /**
     * @description Register an RDF triple with the session
     * @param subject A pointer to a null terminated subject
     * @param predicate A pointer to a null terminated predicate
     * @param object A pointer to a varying length object
     * @param objsize The object size
     */
    int hxhim_iput(char* subject, char* predicate, void* object, size_t objsize);

    /** @description Retrieve*/
    int hxhim_get(char* subject, char* predicate, void* object, size_t objsize);

    /** @description Retrieve*/
    int hxhim_iget(char* subject, char* predicate, void* object, size_t objsize);

    /** @description Retrieve */
    int hxhim_igetv_subjects(char* sbegin, char* send, void** preds, void** objs, size_t* sizes);

    /** @description Retrive */
    int hxhim_igetv_predicates(char** subjects, char* pbegin, void** preds, void** objs, size_t* sizes);


    /** Needs a way to specify an order */
    //int hxhim_create_cursor(size_t offset, size_t count, hxhim_cursor* c);

    /**
     * @description Returns a descriptive string for HXHIM return codes
     * @param the return code to describe
     * @return a string describing the return code
     */
    char* hxhim_strerror(int hxhim_rc);

    /**
     * @description Prints a descriptive string for the return codes
     * @param the return code to print a descriptive string for
     * @return 0
     */
    int hxhim_perror(int hxhim_rc);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif
