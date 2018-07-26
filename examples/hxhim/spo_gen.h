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

// clean up generated SPOs
void spo_clean(const size_t count,
               void **subjects, size_t *subject_lens,
               void **predicates, size_t *predicate_lens,
               void **objects, size_t *object_lens) {
    if (subjects) {
        for(size_t i = 0; i < count; i++) {
            free(subjects[i]);
        }
        free(subjects);
    }

    if (subjects) {
        free(subject_lens);
    }

    if (predicates) {
        for(size_t i = 0; i < count; i++) {
            free(predicates[i]);
        }
        free(predicates);
    }

    if (predicates) {
        free(predicate_lens);
    }

    if (objects) {
        for(size_t i = 0; i < count; i++) {
            free(objects[i]);
        }
        free(objects);
    }

    if (objects) {
        free(object_lens);
    }
}

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
        spo_clean(count, *subjects, *subject_lens, *predicates, *predicate_lens, *objects, *object_lens);
        return 0;
    }

    for(size_t i = 0; i < count; i++) {
        if (!((*subjects)[i] = calloc(bufsize, sizeof(char)))   ||
            !((*predicates)[i] = calloc(bufsize, sizeof(char))) ||
            !((*objects)[i] = calloc(bufsize, sizeof(char))))    {
            spo_clean(count, *subjects, *subject_lens, *predicates, *predicate_lens, *objects, *object_lens);
            return 0;
        }

        (*subject_lens)[i] = snprintf((char *) (*subjects)[i], bufsize, "subject%d%zu", rank, i);
        (*predicate_lens)[i] = snprintf((char *) (*predicates)[i], bufsize, "predicate%d%zu", rank, i);
        (*object_lens)[i] = snprintf((char *) (*objects)[i], bufsize, "object%d%zu", rank, i);
    }

    return count;
}

int spo_gen_random(const size_t count,
                   const size_t subject_min_size, const size_t subject_max_size,
                   void ***subjects, size_t **subject_lens,
                   const size_t predicate_min_size, const size_t predicate_max_size,
                   void ***predicates, size_t **predicate_lens,
                   const size_t object_min_size, const size_t object_max_size,
                   void ***objects, size_t **object_lens) {
    if (!subjects   || !subject_lens              ||
        !predicates || !predicate_lens            ||
        !objects    || !object_lens               ||
        (subject_min_size > subject_max_size)     ||
        (predicate_min_size > predicate_max_size) ||
        (object_min_size > object_max_size))       {
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
        spo_clean(count, *subjects, *subject_lens, *predicates, *predicate_lens, *objects, *object_lens);
        return 0;
    }

    for(size_t i = 0; i < count; i++) {
        (*subject_lens)[i] = (rand() % (subject_max_size - subject_min_size + 1)) + subject_min_size;
        (*subjects)[i] = calloc((*subject_lens)[i] , sizeof(char));

        (*predicate_lens)[i] = (rand() % (predicate_max_size - predicate_min_size + 1)) + predicate_min_size;
        (*predicates)[i] = calloc((*predicate_lens)[i] , sizeof(char));

        (*object_lens)[i] = (rand() % (object_max_size - object_min_size + 1)) + object_min_size;
        (*objects)[i] = calloc((*object_lens)[i] , sizeof(char));

        if (!((*subjects)[i] = calloc((*subject_lens)[i], sizeof(char)))   ||
            !((*predicates)[i] = calloc((*predicate_lens)[i], sizeof(char))) ||
            !((*objects)[i] = calloc((*object_lens)[i], sizeof(char))))    {
            spo_clean(count, *subjects, *subject_lens, *predicates, *predicate_lens, *objects, *object_lens);
            return 0;
        }

        for(size_t j = 0; j < (*subject_lens)[i]; j++) {
            ((char *) (*subjects)[i])[j] = (char) rand();
        }

        for(size_t j = 0; j < (*predicate_lens)[i]; j++) {
            ((char *) (*predicates)[i])[j] = (char) rand();
        }

        for(size_t j = 0; j < (*object_lens)[i]; j++) {
            ((char *) (*objects)[i])[j] = (char) rand();
        }
    }

    return count;
}

#ifdef __cplusplus
}
#endif

#endif
