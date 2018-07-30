#include "utils/elapsed.h"

long double nano(const struct timespec start, const struct timespec end) {
    return (long double) (end.tv_sec - start.tv_sec) +
        ((long double) (end.tv_nsec - start.tv_nsec)/1000000000.0);
}
