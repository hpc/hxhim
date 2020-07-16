#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "spo_gen.h"

size_t alloc_void_ptr_array(const size_t count,
                            void ***array,
                            size_t **lens) {
    if (!array || !lens) {
        return 0;
    }

    *array = (void **)  calloc(count, sizeof(void *));
    *lens  = (size_t *) calloc(count, sizeof(size_t));
    return count;
}

void dealloc_void_ptr_array(const size_t count,
                            void ***array,
                            size_t **lens) {
    if (array) {
        if (*array) {
            for(size_t i = 0; i < count; i++) {
                free((*array)[i]);
            }
        }

        free(*array);
        *array = NULL;
    }

    if (lens) {
        free(*lens);
        *lens = NULL;
    }
}

size_t gen_fixed_pattern(void **dst, size_t *len,
                         const size_t max_len, const char *pattern,
                         const int rank, const size_t idx) {
    if (!dst || !len) {
        return 0;
    }

    char *buf = (char *) calloc(max_len, sizeof(char));
    *len = snprintf(buf, max_len, pattern, rank, idx);
    *dst = buf;
    return *len;
}

size_t spo_gen_fixed(const size_t count, const size_t bufsize,
                     const int rank,
                     void ***subjects, size_t **subject_lens,
                     void ***predicates, size_t **predicate_lens,
                     void ***objects, size_t **object_lens) {
    alloc_void_ptr_array (count, subjects,   subject_lens);
    alloc_void_ptr_array (count, predicates, predicate_lens);
    alloc_void_ptr_array (count, objects,    object_lens);

    if ((subjects   && !*subjects)   || (subject_lens   && !*subject_lens)   ||
        (predicates && !*predicates) || (predicate_lens && !*predicate_lens) ||
        (objects    && !*objects)    || (object_lens    && !*object_lens))    {
        dealloc_void_ptr_array(count, subjects,   subject_lens);
        dealloc_void_ptr_array(count, predicates, predicate_lens);
        dealloc_void_ptr_array(count, objects,    object_lens);
        return 0;
    }

    if (subjects) {
        for(size_t i = 0; i < count; i++) {
            gen_fixed_pattern(&(*subjects)[i], &(*subject_lens)[i],
                              bufsize, "subject-%d-%zu", rank, i);
        }
    }

    if (predicates) {
        for(size_t i = 0; i < count; i++) {
            gen_fixed_pattern(&(*predicates)[i], &(*predicate_lens)[i],
                              bufsize, "predicate-%d-%zu", rank, i);
        }
    }

    if (objects) {
        for(size_t i = 0; i < count; i++) {
            gen_fixed_pattern(&(*objects)[i], &(*object_lens)[i],
                              bufsize, "object-%d-%zu", rank, i);
        }
    }

    return count;
}

size_t gen_random_pattern(void **dst, size_t *len,
                          const size_t min_len, const size_t max_len) {
    if (!dst || !len) {
        return 0;
    }

    *len = rand() % (max_len - min_len + 1) + min_len;
    char *buf = (char *) calloc(*len, sizeof(char));
    for(size_t i = 0; i < *len; i++) {
        buf[i] = rand();
    }
    *dst = buf;
    return *len;
}

size_t spo_gen_random(const size_t count,
                      void ***subjects, size_t **subject_lens,
                      const size_t subject_min_size, const size_t subject_max_size,
                      void ***predicates, size_t **predicate_lens,
                      const size_t predicate_min_size, const size_t predicate_max_size,
                      void ***objects, size_t **object_lens,
                      const size_t object_min_size, const size_t object_max_size) {
    alloc_void_ptr_array (count, subjects,   subject_lens);
    alloc_void_ptr_array (count, predicates, predicate_lens);
    alloc_void_ptr_array (count, objects,    object_lens);

    if ((subjects   && !*subjects)   || (subject_lens   && !*subject_lens)   ||
        (predicates && !*predicates) || (predicate_lens && !*predicate_lens) ||
        (objects    && !*objects)    || (object_lens    && !*object_lens))    {
        dealloc_void_ptr_array(count, subjects,   subject_lens);
        dealloc_void_ptr_array(count, predicates, predicate_lens);
        dealloc_void_ptr_array(count, objects,    object_lens);
        return 0;
    }

    if (subjects) {
        for(size_t i = 0; i < count; i++) {
            gen_random_pattern(&(*subjects)[i], &(*subject_lens)[i],
                               subject_min_size, subject_max_size);
        }
    }

    if (predicates) {
        for(size_t i = 0; i < count; i++) {
            gen_random_pattern(&(*predicates)[i], &(*predicate_lens)[i],
                               predicate_min_size, predicate_max_size);
        }
    }

    if (objects) {
        for(size_t i = 0; i < count; i++) {
            gen_random_pattern(&(*objects)[i], &(*object_lens)[i],
                               object_min_size, object_max_size);
        }
    }

    return count;
}

void spo_clean(const size_t count,
               void ***subjects, size_t **subject_lens,
               void ***predicates, size_t **predicate_lens,
               void ***objects, size_t **object_lens) {
    dealloc_void_ptr_array(count, subjects,   subject_lens);
    dealloc_void_ptr_array(count, predicates, predicate_lens);
    dealloc_void_ptr_array(count, objects,    object_lens);
}
