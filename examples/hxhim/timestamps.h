#ifndef HXHIM_EXAMPLES_TIMESTAMPS_H
#define HXHIM_EXAMPLES_TIMESTAMPS_H

#include <inttypes.h>
#include <mpi.h>
#include <stdint.h>
#include <time.h>

uint64_t nano(struct timespec *start, struct timespec *end);
long double sec(struct timespec *start, struct timespec *end);

#define timestamp_start_no_create(name)             \
    clock_gettime(CLOCK_MONOTONIC, &name ## _start)

#define timestamp_end_no_create(name)                               \
    clock_gettime(CLOCK_MONOTONIC, &name ## _end);                  \
    fprintf(stderr, "%d " #name " %" PRIu64 " %" PRIu64 " %.3Lf\n", \
            rank,                                                   \
            nano(&epoch, &name ## _start),                          \
            nano(&epoch, &name ## _end),                            \
            sec(&name ## _start, &name ## _end))                    \

#define timestamp_start(name)                   \
    struct timespec name ## _start;             \
    timestamp_start_no_create(name)

#define timestamp_end(name)                     \
    struct timespec name ## _end;               \
    timestamp_end_no_create(name)

#define barrier                                 \
    do {                                        \
        timestamp_start(mpi_barrier);           \
        MPI_Barrier(MPI_COMM_WORLD);            \
        timestamp_end(mpi_barrier);             \
    } while(0)

#endif
