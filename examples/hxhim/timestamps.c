#include "timestamps.h"

uint64_t nano(struct timespec *start, struct timespec *end) {
    uint64_t s = start->tv_sec;
    s *= 1000000000ULL;
    s += start->tv_nsec;

    uint64_t e = end->tv_sec;
    e *= 1000000000ULL;
    e += end->tv_nsec;

    return e - s;
}

long double sec(struct timespec *start, struct timespec *end) {
    return (long double) nano(start, end) / 1e9;
}
