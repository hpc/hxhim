#ifndef STATS_H
#define STATS_H

#include <stdint.h>
#include <time.h>

#ifdef __cplusplus
extern "C"
{
#endif

/** Serializing timespecs makes more sense than serializing std::chronos */
/** Also used for C structs */
struct Monostamp {
    struct timespec start;
    struct timespec end;
};

long double elapsed(const struct Monostamp *duration);
long double elapsed2(const struct timespec *start, const struct timespec *end);

uint64_t nano(const struct Monostamp *duration);
uint64_t nano2(const struct timespec *start, const struct timespec *end);

#ifdef __cplusplus
}
#endif

#endif
