#ifndef HXHIM_EXAMPLES_SPO_GEN
#define HXHIM_EXAMPLES_SPO_GEN

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#ifdef __cplusplus
extern "C"
{
#endif

// Generate some subject predicate pairs
int spo_gen_fixed(const size_t count, const size_t bufsize,
                  const int rank,
                  void ***subjects, size_t **subject_lens,
                  void ***predicates, size_t **predicate_lens,
                  void ***objects, size_t **object_lens) {
    if (!subjects   || !subject_lens   ||
        !predicates || !predicate_lens ||
        !objects    || !object_lens)    {
        return 0;
    }

    *subjects = (void **) calloc(count, sizeof(void *));
    *subject_lens = (size_t *) calloc(count, sizeof(size_t));
    *predicates = (void **) calloc(count, sizeof(void *));
    *predicate_lens = (size_t *) calloc(count, sizeof(size_t));
    *objects = (void **) calloc(count, sizeof(void *));
    *object_lens = (size_t *) calloc(count, sizeof(size_t));

    if (!*subjects   || !*subject_lens   ||
        !*predicates || !*predicate_lens ||
        !*objects    || !*object_lens)    {
        free(*subjects);
        free(*subject_lens);
        free(*predicates);
        free(*predicate_lens);
        free(*objects);
        free(*object_lens);
        return 0;
    }

    for(size_t i = 0; i < count; i++) {
        (*subjects)[i] = calloc(bufsize, sizeof(char));
        (*subject_lens)[i] = snprintf((char *) (*subjects)[i], bufsize, "subject%d%zu", rank, i);
        (*predicates)[i] = calloc(bufsize, sizeof(char));
        (*predicate_lens)[i] = snprintf((char *) (*predicates)[i], bufsize, "predicate%d%zu", rank, i);
        (*objects)[i] = calloc(bufsize, sizeof(char));
        (*object_lens)[i] = snprintf((char *) (*objects)[i], bufsize, "object%d%zu", rank, i);
    }

    return count;
}

int spo_gen_random(const size_t count,
                   const size_t min_size, const size_t max_size,
                   void ***subjects, size_t **subject_lens,
                   void ***predicates, size_t **predicate_lens,
                   void ***objects, size_t **object_lens) {
    if (!subjects   || !subject_lens   ||
        !predicates || !predicate_lens ||
        (min_size > max_size))  {
        return 0;
    }

    *subjects = (void **) calloc(count, sizeof(void *));
    *subject_lens = (size_t *) calloc(count, sizeof(size_t));
    *predicates = (void **) calloc(count, sizeof(void *));
    *predicate_lens = (size_t *) calloc(count, sizeof(size_t));
    *objects = (void **) calloc(count, sizeof(void *));
    *object_lens = (size_t *) calloc(count, sizeof(size_t));

    if (!*subjects   || !*subject_lens   ||
        !*predicates || !*predicate_lens ||
        !*objects    || !*object_lens)    {
        free(*subjects);
        free(*subject_lens);
        free(*predicates);
        free(*predicate_lens);
        free(*objects);
        free(*object_lens);
        return 0;
    }

    for(size_t i = 0; i < count; i++) {
        (*subject_lens)[i] = (rand() % (max_size - min_size + 1)) + min_size;
        (*subjects)[i] = calloc((*subject_lens)[i] , sizeof(char));
        for(size_t j = 0; j < (*subject_lens)[i]; j++) {
            ((char *) (*subjects)[i])[j] = (char) rand();
        }

        (*predicate_lens)[i] = (rand() % (max_size - min_size + 1)) + min_size;
        (*predicates)[i] = calloc((*predicate_lens)[i] , sizeof(char));
        for(size_t j = 0; j < (*predicate_lens)[i]; j++) {
            ((char *) (*predicates)[i])[j] = (char) rand();
        }

        (*object_lens)[i] = (rand() % (max_size - min_size + 1)) + min_size;
        (*objects)[i] = calloc((*object_lens)[i] , sizeof(char));
        for(size_t j = 0; j < (*object_lens)[i]; j++) {
            ((char *) (*objects)[i])[j] = (char) rand();
        }
    }

    return count;
}

// clean up generated predicates
void spo_clean(const size_t count,
               void **subjects, size_t *subject_lens,
               void **predicates, size_t *predicate_lens,
               void **objects, size_t *object_lens) {
    for(size_t i = 0; i < count; i++) {
        free(subjects[i]);
        free(predicates[i]);
        free(objects[i]);
    }

    free(subjects);
    free(subject_lens);
    free(predicates);
    free(predicate_lens);
    free(objects);
    free(object_lens);
}

#ifdef __cplusplus
}
#endif

#endif
