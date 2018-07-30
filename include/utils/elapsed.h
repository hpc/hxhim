#ifndef ELAPSED_H
#define ELAPSED_H

#ifdef __cplusplus
#include <ctime>
extern "C"
{
#else
#include <time.h>
#endif

long double nano(const struct timespec start, const struct timespec end);

#ifdef __cplusplus
}
#endif

#endif
