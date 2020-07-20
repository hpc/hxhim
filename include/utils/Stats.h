#ifndef STATS_H
#define STATS_H

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

#ifdef __cplusplus
}
#endif

#endif
