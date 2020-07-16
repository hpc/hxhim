#ifndef HXHIM_EXAMPLES_SPO_GEN
#define HXHIM_EXAMPLES_SPO_GEN

#include <stddef.h>

#ifdef __cplusplus
extern "C"
{
#endif

/** @description Allocate a void ** + size_t * pair and store the addresses at array and lens*/
size_t alloc_void_ptr_array(const size_t count,
                            void ***array,
                            size_t **lens);

/** @description Deallocate a void ** + size_t *ptr and set the pointers to NULL */
void dealloc_void_ptr_array(const size_t count,
                         void ***array,
                         size_t **lens);

/** @description Set a single void * to contain a fixed pattern containing the rank and index */
size_t gen_fixed_pattern(void **dst, size_t *len,
                         const size_t max_len, const char *pattern,
                         const int rank, const size_t idx);

/** @description Generate fixed SPO triples */
size_t spo_gen_fixed(const size_t count, const size_t bufsize,
                     const int rank,
                     void ***subjects, size_t **subject_lens,
                     void ***predicates, size_t **predicate_lens,
                     void ***objects, size_t **object_lens);

/** @description Set a single void * to contain random data */
size_t gen_random_pattern(void **dst, size_t *len,
                          const size_t min_len, const size_t max_len);

/** @description Generate random data for SPO triples */
size_t spo_gen_random(const size_t count,
                      void ***subjects, size_t **subject_lens,
                      const size_t subject_min_size, const size_t subject_max_size,
                      void ***predicates, size_t **predicate_lens,
                      const size_t predicate_min_size, const size_t predicate_max_size,
                      void ***objects, size_t **object_lens,
                      const size_t object_min_size, const size_t object_max_size);

/** @description Clean up SPO triples */
void spo_clean(const size_t count,
               void ***subjects, size_t **subject_lens,
               void ***predicates, size_t **predicate_lens,
               void ***objects, size_t **object_lens);

#ifdef __cplusplus
}
#endif

#endif
