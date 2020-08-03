#ifndef HXHIM_EXAMPLES_TIMESTAMPS_H
#define HXHIM_EXAMPLES_TIMESTAMPS_H

#include <inttypes.h>
#include <stdint.h>
#include <time.h>

uint64_t nano(struct timespec *start, struct timespec *end);
long double sec(struct timespec *start, struct timespec *end);

#define timestamp_start(name)                                       \
    struct timespec name ## _start;                                 \
    clock_gettime(CLOCK_MONOTONIC, &name ## _start)                 \

#define timestamp_end(name)                                         \
    struct timespec name ## _end;                                   \
    clock_gettime(CLOCK_MONOTONIC, &name ## _end);                  \
    fprintf(stderr, "%d " #name " %" PRIu64 " %" PRIu64 " %Lf\n",   \
            rank,                                                   \
            nano(&epoch, &name ## _start),                          \
            nano(&epoch, &name ## _end),                            \
            sec(&name ## _start, &name ## _end))                    \

#endif
