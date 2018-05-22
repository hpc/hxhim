#ifndef HXHIM_EXAMPLES_KV_GEN
#define HXHIM_EXAMPLES_KV_GEN

#include <stdint.h>
#include <stdio.h>

// Generate some key value pairs
int kv_gen(const size_t count, const size_t bufsize,
            const int rank,
            void ***keys, size_t **key_lens,
            void ***values, size_t **value_lens) {
    *keys = (void **) calloc(count, sizeof(void *));
    *key_lens = (size_t *) calloc(count, sizeof(size_t));
    *values = (void **) calloc(count, sizeof(void *));
    *value_lens = (size_t *) calloc(count, sizeof(size_t));

    if (!keys   || !key_lens    ||
        !values || !value_lens ) {
        free(keys);
        free(key_lens);
        free(values);
        free(value_lens);
        return 0;
    }

    for(size_t i = 0; i < count; i++) {
        (*keys)[i] = calloc(bufsize, sizeof(char));
        (*key_lens)[i] = snprintf((char *) (*keys)[i], bufsize, "key%d%zu", rank, i);
        (*values)[i] = calloc(bufsize, sizeof(char));
        (*value_lens)[i] = snprintf((char *) (*values)[i], bufsize, "value%d%zu", rank, i);
    }

    return count;
}

// clean up generated values
void kv_clean(const size_t count,
              void **keys, size_t *key_lens,
              void **values, size_t *value_lens) {
    for(size_t i = 0; i < count; i++) {
        free(keys[i]);
        free(values[i]);
    }

    free(keys);
    free(key_lens);
    free(values);
    free(value_lens);
}

#endif
